#!/usr/bin/env python

"""
Copyright: Travis Cammack 2017
Original Author: Travis Cammack
Create Date: 5/6/2017
Contributors:
################################################################################

This is the driver for datagrab
"""


import requests
from utils import *
from config import *
import os
import sys
import symbols
import price


if __name__ == '__main__':

    master_dates_list = get_dates(DATA_RANGE)

    try:
        symbols.run()
        price.run(master_dates_list, 10)
    except requests.ConnectionError:
        print "could not connect to the interwebs"
