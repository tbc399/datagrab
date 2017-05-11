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
from config import *


def run():
    """Entry point for price

    TODO
    """

    with open(SECTOR_MAPPING_FILE, 'r') as f:
        try:
            sector_mapping = json.load(f)
        except ValueError:
            print "ERROR: could not load sector mapping file as JSON"
            return False
