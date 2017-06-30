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
import logging
import json
import os
from datetime import date, datetime, timedelta
from config import *


HEADERS = {
    "Authorization": "Bearer {}".format(TRADIER_BEARER_TOKEN),
    "Accept": "application/json"
}


def __get_formatted_date_range():
    """Return data date range
    
    TODO
    """

    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=DATA_RANGE)

    end_date_string = end_date.strftime("%Y-%m-%d")
    start_date_string = start_date.strftime("%Y-%m-%d")

    return start_date_string, end_date_string


def __write_out_symbol_data(symbol, data, sector_dir, description=""):
    """Write symbol prices to JSON file
    
    TODO
    """

    data_values = [item["value"] for item in data]

    data = {
        "name": symbol,
        "description": description,
        "interval_type": "DAY",
        "interval": 1,
        "minimum": min(data_values),
        "maximum": max(data_values),
        "data": data
    }

    file_name = os.path.join(sector_dir, "{name}.sym.json".format(name=symbol))

    with open(file_name, 'w') as f:
        json.dump(data, f, indent=2)


def __write_out_dependent_data(name, symbol, data, sector_dir, description=""):
    """Write symbol dependent data to JSON
    
    TODO
    """

    data_values = [item["value"] for item in data]

    data = {
        "name": name,
        "description": description,
        "interval_type": "DAY",
        "interval": 1,
        "symbol_dependency": symbol,
        "minimum": min(data_values),
        "maximum": max(data_values),
        "data": data
    }

    file_name = os.path.join(
        sector_dir,
        "{sym}_{name}.dep.json".format(sym=symbol, name=name)
    )

    with open(file_name, 'w') as f:
        json.dump(data, f, indent=2)


def __download_symbol_price_and_volume(symbol, sector_dir):
    """Download a single symbol's closing price

    TODO
    """

    print "Downloading price and volume for {sym}".format(sym=symbol)

    start_date, end_date = __get_formatted_date_range()

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

    __write_out_symbol_data(
        symbol,
        price,
        sector_dir,
        description="The closing price"
    )
    __write_out_dependent_data(
        "Volume",
        symbol,
        volume,
        sector_dir,
        description="The daily volume"
    )


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

            __download_symbol_price_and_volume(symbol, sector_dir)