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
from config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST
from config import START_DATE, TZ
from config import load
from datetime import datetime, timedelta, timezone
import symbols
import price
from psycopg2 import connect
import sys


if __name__ == '__main__':

    #  load in the configuration from the command line
    try:
        load(sys.argv[1])
    except IndexError:
        print('Need a json config file to read from', file=sys.stderr)
        exit(-1)

    #  get one master connection to the database that will be passed around
    # try:
    #     db_connection = psycopg2.connect(
    #             dbname=DB_NAME,
    #             user=DB_USER,
    #             password=DB_PASSWORD,
    #             host=DB_HOST
    #         )
    # except Exception as e:
    #     print('Failed to connect to the database: {}'.format(e))
    #     exit(-1)
    
    db_credentials = dict(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
        )
    with connect(db_credentials) as conn:

        #  a list of tuples of the form (<symbol_name>, <sector_code>)
        symbol_names = symbols.run()

        #  get all valid open market dates within a range
        tz_offset = timezone(timedelta(hours=TZ))
        end = datetime.now(tz=tz_offset).date()
        start = datetime.strptime(START_DATE, "%Y-%m-%d").date()
        master_dates_list = get_valid_market_dates(start, end)

        #  update the prices for all symbols in symbol_names in th db
        price.run(conn, symbol_names, master_dates_list)
