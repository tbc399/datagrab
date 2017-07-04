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


def __download_symbol_price_and_volume(symbol, dates_list, sector_dir, lag):
    """Download a single symbol's closing price and volume

    TODO
    """

    print "Downloading price and volume for {sym}".format(sym=symbol)

    start_date = dates_list[0].strftime("%Y-%m-%d")
    end_date = dates_list[-1].strftime("%Y-%m-%d")

    extended_dates_list = get_number_of_weekdays(
        start_date - timedelta(days=1), lag
    ).extend(list(dates_list))

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

    json_response = json.loads(response.text)

    if not json_response:
        print "WARNING: could not download data for symbol {}".format(symbol)
        print "WARNING: status code {code}: {body}".format(
            code=response.status_code,
            body=response.text
        )
        return

    price = [
        {"date": day["date"], "value": day["close"]} for day in
        json_response["history"]["day"]
    ]

    volume = [
        {"date": day["date"], "value": day["volume"]} for day in
        json_response["history"]["day"]
    ]

    write_out_symbol_data(
        symbol,
        price,
        sector_dir,
        description="The closing price"
    )
    write_out_dependent_data(
        "Volume",
        symbol,
        volume,
        sector_dir,
        description="The daily volume"
    )


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
