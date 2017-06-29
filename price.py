"""
Copyright: Travis Cammack 2017
Original Author: Travis Cammack
Create Date: 5/8/2017
Contributors:
################################################################################

This module grabs standard historical prices for a given set of symbols.
"""


import requests
import json
import os
from config import *


def __download_symbol_price(symbol, sector_dir):
    """Download a single symbol's closing price

    TODO
    """

    url = "".format()


def run():
    """Entry point for price

    TODO
    """

    sector_file = os.path.join(DATA_DOWNLOAD_DIR, SECTOR_MAPPING_FILE)
    with open(sector_file, 'r') as f:
        try:
            sector_mapping = json.load(f)
        except ValueError:
            print "ERROR: could not load sector mapping file as JSON"
            return False

    for sector_code, sector_details in sector_mapping.iteritems():

        sector_dir = os.path.join(DATA_DOWNLOAD_DIR, sector_details["name"])
        if not os.path.exists(sector_dir):
            os.mkdir(sector_dir)

        for symbol in sector_details["symbols"]:

            __download_symbol_price(symbol, sector_dir)