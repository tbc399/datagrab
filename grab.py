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
import os
import sys
import symbols


if __name__ == '__main__':
    try:
        symbols.run()
    except requests.ConnectionError:
        print "could not connect to the interwebs"
