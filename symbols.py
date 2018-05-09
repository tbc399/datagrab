"""
Copyright: Travis Cammack 2017
Original Author: Travis Cammack
Create Date: 5/6/2017
Contributors:
################################################################################

This module grabs symbols from NYSE and NASDAQ and puts them
into their respective sectors.
"""


import json
import requests
import config
import string


MORNINGSTAR_SECTOR_CODES = {
    101: 'Basic Materials',
    102: 'Consumer Cyclical',
    103: 'Financial Services',
    104: 'Real Estate',
    205: 'Consumer Defensive',
    206: 'Healthcare',
    207: 'Utilities',
    308: 'Communication Services',
    309: 'Energy',
    310: 'Industrials',
    311: 'Technology',
}



def _validate_symbol(symbol):
    """Validate a stock symbol

    Return true if this symbol doesn't have any funky stuff going
    on with it like ABDn:BXRT or something like that
    """

    for c in symbol:
        if c < 'A' or c > 'Z':
            return False

    return True


def _get_symbols(character):
    """Grabs stock symbols by character
    
    This function takes in a single alphabetic character
    and generates a list of stock symbols that begin with
    that character. For now, it only pulls from NASDAQ
    and NYSE.
    """

    if 'a' > character.lower() > 'z':
        raise ValueError("'{}' is not a character".format(character))

    url = "https://{host}/{version}/markets/lookup".format(
        host=config.TRADIER_API_DOMAIN,
        version=config.TRADIER_API_VERSION
    )
    query = {
        'q': character,
        'exchanges': 'N,Q',
        'types': 'stock'
    }
    headers = {
        "Authorization": "Bearer {}".format(config.TRADIER_BEARER_TOKEN),
        "Accept": "application/json"
    }

    response = requests.get(url, params=query, headers=headers)

    if response.status_code != 200:
        raise IOError(
            "there was a network problem getting "
            "the symbols for character {char}".format(char=character)
        )

    json_response = json.loads(response.text)

    #  list of returned symbols
    securities = json_response['securities']['security']

    for security in securities:
        symbol = security['symbol']
        if _validate_symbol(symbol):
            yield symbol


def __split_into_sector(symbols):
    """Split each symbol into its sector
    
    TODO
    """

    url = "https://{host}/{version}/markets/fundamentals/company".format(
        host=config.TRADIER_API_DOMAIN,
        version=config.TRADIER_BETA_VERSION
    )

    headers = {
        "Authorization": "Bearer {}".format(config.TRADIER_BEARER_TOKEN),
        "Accept": "application/json"
    }
    
    symbol_sector_pairs = []
    symbol_chunks = [symbols[i:i+20] for i in range()]

    for symbol_list in symbol_chunks:

        query = {
            'symbols': symbol_list,
        }

        response = requests.get(url, params=query, headers=headers)

        if response.status_code != 200:
            raise IOError(
                "there was a network problem getting "
                "the sectors of some symbols:\n'{symbols}'".format(
                    symbols=','.join(symbol_list)
                )
            )

        company_infos = response.json()

        for entry in company_infos:

            symbol = entry['request']

            if "error" in entry:
                msg = 'WARNING: Tradier error in getting symbol {}: {}'
                print(msg.format(symbol, entry["error"]))
                continue

            try:
                sector_code = entry['results'][0]['tables'][
                    'asset_classification']['morningstar_sector_code']
            except TypeError:
                print("WARNING: no sector info on {sym}. Skipping".format(
                    sym=symbol
                ))
                continue
            except KeyError as error:
                print("WARNING: could not get a json field '{}'".format(error))
                print(json.dumps(entry, indent=2))
                continue
            else:
                symbol_sector_pairs.append((symbol, sector_code))

    return symbol_sector_pairs


def run():
    """Entry point for symbols
    
    TODO
    """

    symbols_list = []

    for char in string.ascii_uppercase:
        for symbol in _get_symbols(char):
            symbols_list.append(symbol)

    symbol_sector_pairs = __split_into_sector(symbols_list)

    return symbol_sector_pairs
