#!/usr/bin/env python

"""
Copyright: Travis Cammack 2017
Original Author: Travis Cammack
Create Date: 5/6/2017
Contributors:
################################################################################

This is the driver for datagrab
"""


from requests import ConnectionError
from utils import *
from config import *
from datetime import datetime
import symbols
import price


if __name__ == '__main__':

    try:
        # connect to the database
        pass

    except:
        # handle can't connect
        pass

    try:
        symbols_list = symbols.run()
        print(symbols_list)
        print('Gathering valid market open dates...')
        master_dates_list = get_valid_market_dates(
            datetime.strptime(LAST_DATE, "%Y-%m-%d").date(),
            DATA_RANGE
        )
        print(master_dates_list)
        #symbols_list = ["COWNL"]
        #price.run(symbols_list, master_dates_list, 10)
    except ConnectionError:
        print('could not connect to the interwebs')
