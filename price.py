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
import aiohttp
import asyncio
import config
import json
from asyncio_throttle import Throttler
from utils import get_valid_market_dates
from datetime import datetime, timedelta


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
        for i in range(start_ndx, end_ndx):
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


def __fill_in_missing_data(dates_list, incomplete_data, symbol, sector):
    """TODO"""

    ndx_1 = 0
    ndx_2 = 0
    len_1 = len(dates_list)
    len_2 = len(incomplete_data)
    complete_data = []
    for i in dates_list:
        print(i)
    while ndx_1 < len_1 and ndx_2 < len_2:
        if dates_list[ndx_1] == incomplete_data[ndx_2][1]:
            complete_data.append(incomplete_data[ndx_2])
            ndx_2 += 1
        else:
            complete_data.append(
                (symbol, dates_list[ndx_1], sector,
                    None, None, None, None, None)
            )
        ndx_1 += 1

    #  finish filling in complete_data with null if we've reached
    #  the end of incomplete_data
    while ndx_1 < len_1:
        complete_data.append(
            (symbol, dates_list[ndx_1], sector,
                None, None, None, None, None)
        )
        ndx_1 += 1

    assert len(complete_data) == len(dates_list)

    #  now patch up the holes in the data
    #start_patch_ndx = None

    #for i in range(len(complete_data)):
    #    if complete_data[i][3] is None:
    #        if start_patch_ndx is None:
    #            start_patch_ndx = i
    #    else:
    #        if start_patch_ndx is not None:
    #            #  patch
    #            if not __patch_data(start_patch_ndx, i, complete_data):
    #                return []
    #            #  reset for the next patch
    #            start_patch_ndx = None

    #if start_patch_ndx is not None:
    #    if not __patch_data(
    #            start_patch_ndx,
    #            len(complete_data),
    #            complete_data):
    #        return []
    for i in complete_data:
        print(i)
    return complete_data


def __format_prices(prices_json, symbol, sector_code, dates):
    """Transform prices to tuples

    Take in a json object of prices for a given name and
    transform it into a list of tuples to be inserted into
    the db
    """

    if not prices_json['history'] or not prices_json['history']['day']:
        print("WARNING: no history found for {}".format(symbol))
        return []

    returned_data = prices_json["history"]["day"]

    #  replace any "NaN" values with null
    for dict_obj in returned_data:
        for key in dict_obj:
            if dict_obj[key] == 'NaN':
                dict_obj[key] = None

    def to_cents(x): return int(x * 10)

    #  format the Tradier price into [date, price] values
    price_data = [
        (
            symbol,
            datetime.strptime(day["date"], "%Y-%m-%d").date(),
            sector_code,
            to_cents(day['open']),
            to_cents(day['high']),
            to_cents(day['low']),
            to_cents(day['close']),
            day['volume']
        ) for day in returned_data
    ]

    #  fill in any holes in the price data
    complete_price_data = __fill_in_missing_data(
        dates,
        price_data,
        symbol,
        sector_code
    )

    if not complete_price_data:
        #  holes in the data are too big, so skip
        print("WARNING: {} has too many missing data points. skipping").format(
            symbol
        )
        return []

    return complete_price_data


def __get_missing_dates_range(db_conn, name, dates):
    """Get date range

    This guy gets the date range that we need to grab from Tradier.
    it compares the dates in th db with dates (the valid market dates)
    and gets the set difference of the two.
    """

    cursor = db_conn.cursor()
    cursor.execute('SELECT Date FROM stock_prices WHERE Name = %s', (name,))
    db_dates = cursor.fetchall()
    cursor.close()

    missing_dates = set(dates) - set(db_dates)

    start_date = min(missing_dates)
    end_date = max(missing_dates)

    return start_date, end_date


async def __download_prices(session, db_conn, throttle, symbol, dates):
    """Download a symbol's prces

    TODO
    """

    name, sector = symbol

    start_date, end_date = __get_missing_dates_range(db_conn, name, dates)

    url = "https://{host}/{version}/markets/history".format(
        host=config.TRADIER_API_DOMAIN,
        version=config.TRADIER_API_VERSION
    )
    query_params = {
        'symbol': name,
        'start': str(start_date),
        'end': str(end_date)
    }
    headers = {
        "Authorization": "Bearer {}".format(config.TRADIER_BEARER_TOKEN),
        "Accept": "application/json"
    }

    prices = None
    while True:
        try:
            print('fetching {}'.format(name))
            async with throttle:
                async with aiohttp.request('GET', url, params=query_params,
                        headers=headers) as resp:
                    prices = await resp.text()
                    if 'Quota Violation' in prices:
                        print('Quota violation...')
                        await asyncio.sleep(0)
                    else:
                        break
        except aiohttp.connector.ClientConnectorError as e:
            print('Connection error. Waiting ...')
            await asyncio.sleep(2)

    if prices is not None:

        prices = json.loads(prices)
        prices_tuples = __format_prices(prices, name, sector, dates)
        for i in prices_tuples:
            print(i)
        #cursor = db_conn.cursor()
        #query = 'INSERT INTO stock_prices' \
        #        '  VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
        #psycopg2.extras.execute_batch(cursor, query, prices_tuples)
        #db_conn.commit()
        #cursor.close()


async def __run_helper(db_conn, loop, symbols, dates):
    """Dole out price requests"""

    #  throttle the requests since Tradier has a rate limit
    throttle = Throttler(rate_limit=5)

    async with aiohttp.ClientSession(loop=loop) as session:
        tasks = [__download_prices(
            session, db_conn, throttle, name, dates) for name in symbols]
        await asyncio.gather(*tasks)


def run(db_conn, symbols, dates):
    """Entry point for price

    TODO
    """

    loop = asyncio.get_event_loop()
    loop.run_until_complete(__run_helper(db_conn, loop, symbols, dates))
    loop.close()
