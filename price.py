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
from datetime import datetime, timedelta
from config import *


__HEADERS = {
    "Authorization": "Bearer {}".format(TRADIER_BEARER_TOKEN),
    "Accept": "application/json"
}

__extended_dates_list = None

#  Only stock symbols within this range will be considered.
__MAX_PRICE = 10
__MIN_PRICE = 0

__MIN_VOLUME = 0
__MAX_VOLUME = 1000000000


#def __download_symbol_price_and_volume(symbol, dates_list, sector_dir, lag):
def __download_symbol_price_and_volume(symbol, dates_list, lag):
    """Download a single symbol's closing price and volume

    TODO
    """

    print("Downloading price and volume for {sym}").format(sym=symbol)

    global __extended_dates_list

    if not __extended_dates_list:
        __extended_dates_list = get_valid_market_dates(
            dates_list[0] - timedelta(days=1), lag
        )
        __extended_dates_list.extend(list(dates_list))

    start_date = __extended_dates_list[0].strftime("%Y-%m-%d")
    end_date = __extended_dates_list[-1].strftime("%Y-%m-%d")

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

    response = requests.get(url, headers=__HEADERS)

    if response.status_code != 200:
        raise IOError(
            "there was a network problem getting "
            "the historical pricing from symbol {sym}".format(sym=symbol)
        )

    json_response = response.json()

    if not json_response:
        print("WARNING: could not download data for symbol {}").format(symbol)
        print("WARNING: status code {code}: {body}".format(
            code=response.status_code,
            body=response.text
        ))
        return

    if not json_response["history"] or not json_response["history"]["day"]:
        print("WARNING: no history could be found for symbol {}").format(symbol)
        return

    returned_data = json_response["history"]["day"]

    if not __check_bounds(returned_data):
        print("WARNING: failed bounds check for symbol {}").format(symbol)
        return

    #  format the Tradier price into [date, price] values
    price_data = [
        [
            datetime.strptime(day["date"], "%Y-%m-%d").date(),
            day["close"]
        ] for day in returned_data if day["close"] != "NaN"
    ]

    #  fill in any holes in the price data
    complete_price_data = __fill_in_missing_data(
        __extended_dates_list,
        price_data
    )

    if not complete_price_data:
        #  holes in the data are too big, so skip
        print("WARNING: {} has too many missing data points. skipping").format(
            symbol
        )
        return

    #  normalize the price data
    normalized_price_data = [
        normalize(x, __MIN_PRICE, __MAX_PRICE) for x in complete_price_data
    ]

    #  write out the base symbol (without any lag)
    price = normalized_price_data[lag:DATA_RANGE + lag]

    write_out_symbol_data(
        symbol,
        price,
        # sector_dir,
        description="The closing price"
    )

    for i in xrange(1, lag):
        price = normalized_price_data[lag - i:DATA_RANGE + lag - i]
        write_out_dependent_data(
            "Lagging_{}".format(i),
            symbol,
            price,
            #sector_dir,
            description="The closing price lagging by {} day(s)".format(i)
        )

    #  format the volume data into [date, volume] entries
    volume_data = [
        [
            datetime.strptime(day["date"], "%Y-%m-%d").date(),
            day["volume"]
        ] for day in returned_data if day["close"] != "NaN"
    ]

    #  fill in any holes in the volume data
    complete_volume_data = __fill_in_missing_data(
        __extended_dates_list,
        volume_data
    )

    normalized_volume_data = [
        normalize(x, __MIN_VOLUME, __MAX_VOLUME)
        for x in complete_volume_data[lag:DATA_RANGE + lag]
    ]

    write_out_dependent_data(
        "Volume",
        symbol,
        normalized_volume_data,
        #sector_dir,
        description="The daily volume"
    )


def __check_bounds(tradier_data):
    """Check price and volume bounds

    TODO
    """

    for entry in tradier_data:
        price = entry['close']
        vol = entry['volume']
        if price < __MIN_PRICE or price > __MAX_PRICE:
            return False
        if vol < __MIN_VOLUME or vol > __MAX_VOLUME:
            return False

    return True


def __patch_data(start_ndx, end_ndx, data):
    """TODO
    
    start_ndx is the index of the first null valued entry
    in data, and end_ndx is the index of the first valid
    entry in data after start_ndx.
    """

    assert start_ndx <= end_ndx

    patch_size = end_ndx - start_ndx

    if patch_size > 5:
        #  section is too large to patch
        return False

    if start_ndx > 0 and end_ndx < len(data):
        a = data[start_ndx - 1]
        b = data[end_ndx]
        increment = (b - a) / float(patch_size)
        for i in xrange(start_ndx, end_ndx):
            data[i] = data[i - 1] + increment
    elif start_ndx > 0 and end_ndx == len(data):
        fill_value = data[start_ndx - 1]
        for i in range(start_ndx, end_ndx):
            data[i] = fill_value
    elif start_ndx == 0 and end_ndx < len(data):
        fill_value = data[end_ndx]
        for i in range(end_ndx - 1, -1, -1):
            data[i] = fill_value

    return True


def __fill_in_missing_data(dates_list, incomplete_data):
    """TODO"""

    ndx_1 = 0
    ndx_2 = 0
    len_1 = len(dates_list)
    len_2 = len(incomplete_data)
    complete_data = []

    while ndx_1 < len_1 and ndx_2 < len_2:
        if dates_list[ndx_1] == incomplete_data[ndx_2][0]:
            complete_data.append(incomplete_data[ndx_2][1])
            ndx_2 += 1
        else:
            complete_data.append(None)
        ndx_1 += 1

    #  finish filling in complete_data with null if we've reached
    #  the end of incomplete_data
    while ndx_1 < len_1:
        complete_data.append(None)
        ndx_1 += 1

    assert len(complete_data) == len(dates_list)

    #  now patch up the holes in the data
    start_patch_ndx = None

    for i in xrange(len(complete_data)):
        if complete_data[i] is None:
            if start_patch_ndx is None:
                start_patch_ndx = i
        else:
            if start_patch_ndx is not None:
                #  patch
                if not __patch_data(start_patch_ndx, i, complete_data):
                    return []
                #  reset for the next patch
                start_patch_ndx = None

    if start_patch_ndx is not None:
        if not __patch_data(
                start_patch_ndx,
                len(complete_data),
                complete_data):
            return []

    return complete_data


def run(db_conn, symbols, dates):
#def run(symbols_list, dates_list, lag):
    """Entry point for price

    TODO
    """

    #sector_file = os.path.join(DATA_DOWNLOAD_DIR, SECTOR_MAPPING_FILE)
    #with open(sector_file, 'r') as f:
    #    try:
    #        sector_mapping = json.load(f)
    #    except ValueError:
    #        print "ERROR: could not load sector mapping file as JSON"
    #        return False

    #for sector_code, sector_details in sector_mapping.iteritems():

    #    sector_dir = os.path.join(DATA_DOWNLOAD_DIR, sector_details["name"])
    #    if not os.path.exists(sector_dir):
    #        os.mkdir(sector_dir)

    #    for symbol in sector_details["symbols"]:
    
    for name, sector in symbols:
        db_dates = get_db_dates(db_conn, name)
        missing_dates = set(master_dates_list) - set(db_dates)

    for symbol in symbols_list:
        __download_symbol_price_and_volume(
            symbol,
            dates_list,
            #sector_dir,
            lag
        )
