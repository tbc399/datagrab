"""
Copyright: Travis Cammack 2017
Original Author: Travis Cammack
Create Date: 7/1/2017
Contributors:
################################################################################

This module contains some miscellaneous functionality useful to multiple
other modules
"""

import os
import json
from datetime import timedelta, datetime
import config
import requests


#def write_out_symbol_data(symbol, data, sector_dir, description=""):
def write_out_symbol_data(symbol, data, description=""):
    """Write symbol prices to JSON file

    TODO
    """

    json_data = {
        "name": symbol,
        "description": description,
        "interval_type": "DAY",
        "interval": 1,
        "minimum": min(data),
        "maximum": max(data),
        "data": data
    }

    #file_name = os.path.join(sector_dir, "{name}.sym.json".format(name=symbol))
    file_name = os.path.join(
        DATA_DOWNLOAD_DIR,
        "{name}.sym.json".format(name=symbol)
    )

    with open(file_name, 'w') as f:
        json.dump(json_data, f, indent=2)


#def write_out_dependent_data(name, symbol, data, sector_dir, description=""):
def write_out_dependent_data(name, symbol, data, description=""):
    """Write symbol dependent data to JSON

    TODO
    """

    json_data = {
        "name": name,
        "description": description,
        "interval_type": "DAY",
        "interval": 1,
        "symbol_dependency": symbol,
        "minimum": min(data),
        "maximum": max(data),
        "data": data
    }

    #file_name = os.path.join(
    #    sector_dir,
    #    "{sym}_{name}.dep.json".format(sym=symbol, name=name)
    #)
    file_name = os.path.join(
        DATA_DOWNLOAD_DIR,
        "{sym}_{name}.dep.json".format(sym=symbol, name=name)
    )

    with open(file_name, 'w') as f:
        json.dump(json_data, f, indent=2)


def __month_year_iter(start_month, start_year, end_month, end_year):
    """Helper for year/month iteration

    TODO
    """

    ym_end = 12 * end_year + end_month
    ym_start = 12 * start_year + start_month - 1

    for ym in range(ym_start, ym_end):
        y, m = divmod(ym, 12)
        yield y, m+1


def get_valid_market_dates(start_date, end_date):
    """Return master dates list

    This will go get all of the dates withing the specified range where the
    stock market is open.
    """

    market_open_dates = []

    ym_gen = __month_year_iter(
            start_date.month,
            start_date.year,
            end_date.month,
            end_date.year
        )

    for year, month in ym_gen:
        url = "https://{host}/{version}/markets/calendar".format(
                host=config.TRADIER_API_DOMAIN,
                version=config.TRADIER_API_VERSION
            )
        query = {
            'year': year,
            'month': month
        }
        headers = {
            "Authorization": "Bearer {}".format(config.TRADIER_BEARER_TOKEN),
            "Accept": "application/json"
        }
        response = requests.get(url, params=query, headers=headers)

        if response.status_code != 200:
            raise IOError(
                "there was a network problem getting "
                "the market calendar on {}/{}".format(month, year)
            )
        
        j = response.json()
        print(year, month, "{}/{}".format(response.headers, 4))
        for entry in reversed(j["calendar"]["days"]["day"]):
            print(entry)
            d = datetime.strptime(entry["date"], "%Y-%m-%d").date()

            if d <= end_date and entry["status"] == "open":
                market_open_dates.append(d)
            elif entry["status"] == "holiday":
                print(json.dumps(entry, indent=2))

    return market_open_dates


def get_weekdays_in_range(start, end):
    """Generate list of weekdays
    
    TODO
    """

    dates_list = []

    current_date = end
    while current_date >= start:

        if current_date.weekday() < 5:  # a weekday
            dates_list.append(current_date)

        current_date -= timedelta(days=1)

    dates_list.reverse()

    return dates_list


def get_number_of_weekdays(start, number_of_days):
    """Gets a set number of weekdays
    
    TODO
    """

    dates_list = []
    days_count = 0

    current_date = start
    while days_count < number_of_days:

        if current_date.weekday() < 5:  # a weekday
            dates_list.append(current_date)
            days_count += 1

        current_date -= timedelta(days=1)

    dates_list.reverse()

    return dates_list


def normalize(value, minimum, maximum):
    """Normalize using min and max

    TODO
    """

    return (value - minimum) / (maximum - minimum)
