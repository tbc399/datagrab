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

