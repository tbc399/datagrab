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
from config import *


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
        host=TRADIER_API,
        version=TRADIER_API_VERSION
    )
    query = "q={character}&exchanges=N,Q&types=stock".format(
        character=character
    )
    url = "{uri}?{query}".format(
        uri=uri,
        query=query
    )
    headers = {
        "Authorization": "Bearer {}".format(TRADIER_BEARER_TOKEN),
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers)

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
        if '\'' not in symbol and '/' not in symbol:
            yield symbol


def run():
    begin = ord('A')
    end = ord('Z') + 1
    count = 1
    for c in xrange(begin, end):
        for symbol in _get_symbols(chr(c)):
            print symbol
            count += 1
    print "{} symbols".format(count)