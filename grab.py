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
from utils import get_valid_market_dates
import config
from datetime import datetime, timedelta, timezone
import symbols
import price
from psycopg2 import connect
import sys


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
    with connect(**db_credentials) as conn:

        #  a list of tuples of the form (<symbol_name>, <sector_code>)
        #TESTsymbol_names = symbols.run()
        symbol_names = [('AAPL', 311)]

        #  get all valid open market dates within a range
        tz_offset = timezone(timedelta(hours=config.TZ))
        end = datetime.now(tz=tz_offset).date()
        start = datetime.strptime(config.START_DATE, "%Y-%m-%d").date()
        master_dates_list = get_valid_market_dates(start, end)

        #  asynchronously update the prices for all symbols in
        #  symbol_names in th db
        price.run(conn, symbol_names, master_dates_list)
