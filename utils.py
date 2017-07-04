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
from datetime import date, timedelta
from config import *


def write_out_symbol_data(symbol, data, sector_dir, description=""):
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


def write_out_dependent_data(name, symbol, data, sector_dir, description=""):
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


def get_dates(range):
    """Return master dates list
    
    TODO
    """

    yesterday = date.today() - timedelta(days=1)
    start_date = yesterday - timedelta(days=DATA_RANGE)

    return get_weekdays_in_range(start_date, yesterday)


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
