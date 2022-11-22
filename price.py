'''
Copyright: Travis Cammack 2017
Original Author: Travis Cammack
Create Date: 5/8/2017
Contributors:
################################################################################

This module grabs standard historical prices and volumes for a
given set of symbols.
'''


import requests
import logging
import aiohttp
import asyncio
import config
import json
from psycopg2 import extras
from asyncio_throttle import Throttler
from utils import get_valid_market_dates
from datetime import datetime, timedelta
import time

log = logging.getLogger(__name__)


def __patch_data(start_ndx, end_ndx, data):
    '''TODO

    start_ndx is the index of the first null valued entry
    in data, and end_ndx is the index of the first valid
    entry in data after start_ndx.
    '''

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
    '''TODO'''

    ndx_1 = 0
    ndx_2 = 0
    len_1 = len(dates_list)
    len_2 = len(incomplete_data)
    complete_data = []

    while ndx_1 < len_1 and ndx_2 < len_2:
        if dates_list[ndx_1] == incomplete_data[ndx_2][1]:
            complete_data.append(incomplete_data[ndx_2])
            ndx_2 += 1
        else:
            complete_data.append(
                (symbol, dates_list[ndx_1], sector, None, None, None, None, None)
            )
        ndx_1 += 1

    #  finish filling in complete_data with null if we've reached
    #  the end of incomplete_data
    while ndx_1 < len_1:
        complete_data.append(
            (symbol, dates_list[ndx_1], sector, None, None, None, None, None)
        )
        ndx_1 += 1

    assert len(complete_data) == len(dates_list)

    #  now patch up the holes in the data
    # start_patch_ndx = None

    # for i in range(len(complete_data)):
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

    # if start_patch_ndx is not None:
    #    if not __patch_data(
    #            start_patch_ndx,
    #            len(complete_data),
    #            complete_data):
    #        return []
    return complete_data


def __format_prices(prices_json, symbol, sector_code, dates):
    '''Transform prices to tuples

    Take in a json object of prices for a given name and
    transform it into a list of tuples to be inserted into
    the db
    '''

    if not prices_json['history'] or not prices_json['history']['day']:
        print('WARNING: no history found for {}'.format(symbol))
        return []

    returned_data = prices_json['history']['day']

    #  replace any 'NaN' values with null
    for dict_obj in returned_data:
        for key in dict_obj:
            if dict_obj[key] == 'NaN':
                dict_obj[key] = None

    #  format the Tradier price into [date, price] values
    price_data = [
        (
            symbol,
            datetime.strptime(day['date'], '%Y-%m-%d').date(),
            sector_code,
            round(day['open'] * 100) if day['open'] else None,
            round(day['high'] * 100) if day['high'] else None,
            round(day['low'] * 100) if day['low'] else None,
            round(day['close'] * 100) if day['close'] else None,
            day['volume'] if day['volume'] else None,
        )
        for day in returned_data
    ]

    #  fill in any holes in the price data
    complete_price_data = __fill_in_missing_data(dates, price_data, symbol, sector_code)

    if not complete_price_data:
        #  holes in the data are too big, so skip
        print('WARNING: {} has too many missing data points. skipping').format(symbol)
        return []

    return complete_price_data


def __get_missing_dates(db_conn, name, dates):
    '''Get date range

    This guy gets the date range that we need to grab from Tradier.
    it compares the dates in th db with dates (the valid market dates)
    and gets the set difference of the two.
    '''

    cursor = db_conn.cursor()
    cursor.execute('SELECT Date FROM stock_prices WHERE Name = %s', (name,))
    db_dates = [record[0] for record in cursor]
    cursor.close()

    missing_dates = list(set(dates) - set(db_dates))
    missing_dates.sort()

    return missing_dates


def __remove_duplicates(prices, dates):
    '''Remove duplicate price records

    This function takes in prices which is a list of price record tuples
    and builds a new list of tuples from it that contains on those records
    whose dates/times are those listed in dates.
    '''

    dates_set = set(dates)

    return [x for x in prices if x[1] in dates_set]


def __download_yahoo_prices(session, symbol, dates):
    '''Download stock prices from Yahoo

    Downloads stock price info with volume from Yahoo

    :param session: the Requests Session object
    :param symbol: a tuple of the stock name and sector respectively
    :param valid_dates: a list of the range of dates of interest
    :return: a list of tuples of the prices
    '''

    print('Fetching prices from Yahoo')
    print(dates[0], dates[-1])

    name, sector = symbol

    url = 'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}'.format(
        symbol=name
    )
    query = {
        'formatted': True,
        'lang': 'en-US',
        'region': 'US',
        'interval': '1d',
        'events': 'div|split',
        'period1': str(int(time.mktime(dates[0].timetuple()))),
        'period2': str(int(time.mktime(dates[-1].timetuple()))),
    }

    resp = session.get(url, params=query).json()

    try:
        timestamps = resp['chart']['result'][0]['timestamp']
    except Exception:
        print('ERROR')
        print(json.dumps(resp, indent=2))
        return
    except KeyError:
        print('Error in getting Yahoo prices')
        return
    except IndexError:
        print('Error in getting Yahoo prices')
        return
    except TypeError:
        print('Error in getting Yahoo prices')
        return

    lows = [
        round(x * 100) if x is not None else None
        for x in resp['chart']['result'][0]['indicators']['quote'][0]['low']
    ]
    highs = [
        round(x * 100) if x is not None else None
        for x in resp['chart']['result'][0]['indicators']['quote'][0]['high']
    ]
    closes = [
        round(x * 100) if x is not None else None
        for x in resp['chart']['result'][0]['indicators']['quote'][0]['close']
    ]
    opens = [
        round(x * 100) if x is not None else None
        for x in resp['chart']['result'][0]['indicators']['quote'][0]['open']
    ]
    vols = resp['chart']['result'][0]['indicators']['quote'][0]['volume']

    #  list of name and sector for zipping
    names = [name] * len(vols)
    sectors = [sector] * len(vols)

    dates = [datetime.fromtimestamp(x).date() for x in timestamps]

    return list(zip(names, dates, sectors, opens, highs, lows, closes, vols))


def __prices_complete(price_tuples):
    '''Determine if prices complete

    Runs through a list of pricing tuples and return True if there are
    no 'holes' in the data, i.e. no null values

    :param price_tuples: a list of price tuples
    :return: True if no values in the tuples are None
    '''

    if not price_tuples:
        return False

    return all(all((x is not None) for x in pt) for pt in price_tuples)


def __download_prices(session, db_conn, symbol, valid_dates):
    '''Download a symbol's prces

    TODO
    '''

    name, sector = symbol

    dates = __get_missing_dates(db_conn, name, valid_dates)

    if not dates:
        return

    url = 'https://{host}/{version}/markets/history'.format(
        host=config.TRADIER_API_DOMAIN, version=config.TRADIER_API_VERSION
    )
    query = {'symbol': name, 'start': str(dates[0]), 'end': str(dates[-1])}
    headers = {
        'Authorization': 'Bearer {}'.format(config.TRADIER_API_TOKEN),
        'Accept': 'application/json',
    }

    resp = session.get(url, params=query, headers=headers)

    prices = resp.json()

    try:
        print(name)
        price_tuples = __format_prices(prices, name, sector, dates)
    except TypeError as e:
        print(json.dumps(prices, indent=2))
        print(e)
        price_tuples = []

    if not __prices_complete(price_tuples):
        price_tuples = __download_yahoo_prices(session, symbol, valid_dates)

    if not price_tuples:
        return

    unique_price_tuples = __remove_duplicates(price_tuples, dates)

    cursor = db_conn.cursor()
    query = (
        'INSERT INTO stock_prices'
        '(name, date, sector_code, open, high, low, close, volume)'
        '  VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
    )
    extras.execute_batch(cursor, query, unique_price_tuples)
    db_conn.commit()
    cursor.close()


async def __download_prices_async(session, db_conn, throttle, symbol, valid_dates):
    '''Download a symbol's prces

    TODO
    '''

    name, sector = symbol

    dates = __get_missing_dates(db_conn, name, valid_dates)

    if not dates:
        return

    url = 'https://{host}/{version}/markets/history'.format(
        host=config.TRADIER_API_DOMAIN, version=config.TRADIER_API_VERSION
    )
    query = {'symbol': name, 'start': str(dates[0]), 'end': str(dates[-1])}
    headers = {
        'Authorization': 'Bearer {}'.format(config.TRADIER_API_TOKEN),
        'Accept': 'application/json',
    }

    prices = None
    while True:
        try:
            async with throttle:
                print('fetching {}'.format(name))
                async with session.get(url, params=query, headers=headers) as resp:
                    prices = await resp.text()
                    if 'Quota Violation' in prices:
                        print('Quota violation...')
                        await asyncio.sleep(0)
                    else:
                        break
        except aiohttp.connector.ClientConnectorError as e:
            print('Connection error. Waiting ...')
            await asyncio.sleep(2)
        except asyncio.TimeoutError as e:
            print('TimeoutError: {}'.format(e))
            await asyncio.sleep(2)

    if prices is not None:

        prices = json.loads(prices)
        try:
            print(name)
            price_tuples = __format_prices(prices, name, sector, dates)
        except TypeError as e:
            print(json.dumps(prices, indent=2))
            print(e)
        unique_price_tuples = __remove_duplicates(price_tuples, dates)

        cursor = db_conn.cursor()
        query = (
            'INSERT INTO stock_prices'
            '(name, date, sector_code, open, high, low, close, volume)'
            '  VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
        )
        extras.execute_batch(cursor, query, unique_price_tuples)
        db_conn.commit()
        cursor.close()


async def __run_helper(db_conn, loop, symbols, dates):
    '''Dole out price requests'''

    #  throttle the requests since Tradier has a rate limit
    throttle = Throttler(rate_limit=1)

    async with aiohttp.ClientSession(loop=loop) as session:
        tasks = [
            __download_prices_async(session, db_conn, throttle, name, dates)
            for name in symbols
        ]
        await asyncio.gather(*tasks)


def run(db_conn, symbols, dates):
    '''Entry point for price

    TODO
    '''

    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(__run_helper(db_conn, loop, symbols, dates))
    # loop.close()

    with requests.Session() as session:
        for symbol in symbols:
            __download_prices(session, db_conn, symbol, dates)
