"""
Copyright: Travis Cammack 2017
Original Author: Travis Cammack
Create Date: 5/8/2017
Contributors:
################################################################################

This module grabs standard historical prices and volumes for a
given set of symbols.
"""


import requests
import os
from utils import *
from datetime import date, timedelta
from config import *


HEADERS = {
    "Authorization": "Bearer {}".format(TRADIER_BEARER_TOKEN),
    "Accept": "application/json"
}


__extended_dates_list = None


def __download_symbol_price_and_volume(symbol, dates_list, sector_dir, lag):
    """Download a single symbol's closing price and volume

    TODO
    """

    print "Downloading price and volume for {sym}".format(sym=symbol)

    global __extended_dates_list

    if not __extended_dates_list:
        __extended_dates_list = get_valid_market_dates(
            dates_list[0] - timedelta(days=1), lag
        )
        __extended_dates_list.extend(list(dates_list))

    start_date = __extended_dates_list[0].strftime("%Y-%m-%d")
    end_date = __extended_dates_list[-1].strftime("%Y-%m-%d")

    print start_date, end_date

    uri = "https://{host}/{version}/markets/history".format(
        host=TRADIER_API_DOMAIN,
        version=TRADIER_API_VERSION
    )
    query = "symbol={symb}&start={start}&end={end}".format(
        symb=symbol,
        start=start_date,
        end=end_date
    )
    url = "{uri}?{query}".format(
        uri=uri,
        query=query
    )

    response = requests.get(url, headers=HEADERS)

    if response.status_code != 200:
        raise IOError(
            "there was a network problem getting "
            "the historical pricing from symbol {sym}".format(sym=symbol)
        )

    json_response = response.json()

    if not json_response:
        print "WARNING: could not download data for symbol {}".format(symbol)
        print "WARNING: status code {code}: {body}".format(
            code=response.status_code,
            body=response.text
        )
        return

    returned_data = json_response["history"]["day"]

    complete_data = fill_in_missing_data(__extended_dates_list, returned_data)

    extended_prices = [day["close"] for day in complete_data]

    for i in xrange(lag):

        price = extended_prices[lag - i:DATA_RANGE + lag - i]

        write_out_symbol_data(
            "{}_{}".format(symbol, i),
            price,
            sector_dir,
            description="The closing price lagging by {} day(s)".format(i)
        )

    volume = [day["volume"] for day in complete_data[lag:DATA_RANGE + lag]]

    write_out_dependent_data(
        "Volume",
        symbol,
        volume,
        sector_dir,
        description="The daily volume"
    )


def fill_in_missing_data(dates_list, incomplete_data):
    """TODO"""

    ndx_1 = 0
    ndx_2 = 0
    len_1 = len(dates_list)
    len_2 = len(incomplete_data)

    complete_data = []

    while ndx_1 < len_1 or ndx_2 < len_2:

        if dates_list[ndx_1] == incomplete_data[ndx_2][0]:

        else:
            ndx_2 += 1


def run(dates_list, lag):
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

            __download_symbol_price_and_volume(
                symbol,
                dates_list,
                sector_dir,
                lag
            )
