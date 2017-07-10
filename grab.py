#!/usr/bin/env python

"""
Copyright: Travis Cammack 2017
Original Author: Travis Cammack
Create Date: 5/6/2017
Contributors:
################################################################################

This is the driver for datagrab
"""


import requests
from utils import *
from config import *
from datetime import datetime
import symbols
import price


if __name__ == '__main__':

    try:

        print "Gathering valid market open dates..."
        master_dates_list = get_valid_market_dates(
            datetime.strptime(LAST_DATE, "%Y-%m-%d").date(),
            DATA_RANGE
        )

        #symbols_list = symbols.run()
        symbols_list = ["COWNL"]
        price.run(symbols_list, master_dates_list, 10)

    except requests.ConnectionError:
        print "could not connect to the interwebs"
