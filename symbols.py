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
import logging
from config import *


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

HEADERS = {
    "Authorization": "Bearer {}".format(TRADIER_BEARER_TOKEN),
    "Accept": "application/json"
}


def _get_symbols(character):
    """Grabs stock symbols by character
    
    This function takes in a single alphabetic character
    and generates a list of stock symbols that begin with
    that character. For now, it only pulls from NASDAQ
    and NYSE.
    """

    lchar = character.lower()

    if lchar < 'a' or lchar > 'z':
        raise ValueError("'{}' is not a character".format(character))

    uri = "https://{host}/{version}/markets/lookup".format(
        host=TRADIER_API_DOMAIN,
        version=TRADIER_API_VERSION
    )
    query = "q={character}&exchanges=N,Q&types=stock".format(
        character=character
    )
    url = "{uri}?{query}".format(
        uri=uri,
        query=query
    )

    response = requests.get(url, headers=HEADERS)

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
        if '\'' not in symbol and '/' not in symbol and 'w' not in symbol:
            yield symbol


def _split_into_sector(symbol_lists):
    """Split each symbol into its sector
    
    TODO
    """

    uri = "https://{host}/{version}/markets/fundamentals/company".format(
        host=TRADIER_API_DOMAIN,
        version=TRADIER_BETA_VERSION
    )

    sector_mapping = {}

    for symbol_list in symbol_lists:

        query = "symbols={}".format(','.join(symbol_list))
        url = "{uri}?{query}".format(
            uri=uri,
            query=query
        )

        response = requests.get(url, headers=HEADERS)

        if response.status_code != 200:
            raise IOError(
                "there was a network problem getting "
                "the sectors of some symbols:\n'{symbols}'".format(
                    symbols=','.join(symbol_list)
                )
            )

        company_infos = json.loads(response.text)

        for entry in company_infos:

            symbol = entry['request']

            try:
                sector_code = entry['results'][0]['tables'][
                    'asset_classification']['morningstar_sector_code']
            except TypeError:
                print "WARNING: no sector info on {sym}. Skipping".format(
                    sym=symbol
                )
                continue

            if sector_code in sector_mapping:
                sector_mapping[sector_code]['symbols'].append(symbol)
            else:
                try:
                    sector_name = MORNINGSTAR_SECTOR_CODES[sector_code]
                except KeyError as e:
                    print "WARNING: sector code {code} is not legit!".format(
                        code=sector_code
                    )
                    sector_name = "Unknown"

                sector_mapping[sector_code] = {
                    "name": sector_name,
                    "symbols": [symbol]
                }

    return sector_mapping


def run():
    """Entry point for symbols
    
    TODO
    """

    symbol_lists = [[]]
    index = 0

    for char in "A":#BCDEFGHIJKLMNOPQRSTUVWXYZ":

        for symbol in _get_symbols(char):

            if len(symbol_lists[index]) <= QUERY_SYMBOL_COUNT:
                symbol_lists[index].append(symbol)
            else:
                symbol_lists.append([symbol])
                index += 1

    sector_mapping = _split_into_sector(symbol_lists)

    with open(SECTOR_MAPPING_FILE, 'w') as f:
        json.dump(sector_mapping, f)

    return True
