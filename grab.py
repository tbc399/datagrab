#!/usr/bin/env python

"""
Copyright: Travis Cammack 2017
Original Author: Travis Cammack
Create Date: 5/6/2017
Contributors:
################################################################################

This is the driver for datagrab
"""


from utils import get_start_end_dates, get_valid_market_dates
import config
import symbols
import price
from psycopg2 import connect
import sys
import time
import requests


if __name__ == '__main__':

    #  load in the configuration from the command line
    try:
        config.load(sys.argv[1])
    except IndexError:
        print('Need a json config file to read from', file=sys.stderr)
        exit(-1)
    db_credentials = dict(
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            host=config.DB_HOST
        )

    start_time = time.time()

    with connect(**db_credentials) as conn:

        #  a list of tuples of the form (<symbol_name>, <sector_code>)
        symbol_names = symbols.run()

        #  get all valid open market dates within a range
        start, end = get_start_end_dates(config)
        master_dates_list = get_valid_market_dates(start, end)

        #  update the prices for all symbols in
        #  symbol_names in th db
        price.run(conn, symbol_names, master_dates_list)

    end_time = time.time()
    duration = (end_time - start_time) / 60
    print('Run Duration: {}'.format(duration))
