"""
Copyright: Travis Cammack 2017
Original Author: Travis Cammack
Create Date: 5/6/2017
Contributors:
################################################################################

This module holds some sweet action settings
"""

import os

TRADIER_API_DOMAIN = "api.tradier.com"
TRADIER_API_VERSION = "v1"
TRADIER_BETA_VERSION = "beta"
TRADIER_BEARER_TOKEN = "ey39F8VMeFvhNsq4vavzeQXThcpL"
QUERY_SYMBOL_COUNT = 20
SECTOR_MAPPING_FILE = "sectored.json"
DATA_DOWNLOAD_DIR = os.path.join(os.environ["HOME"], "sonicred/TrainingData")
DATA_RANGE = 730


def validate_configuration():
    """Validates config variables
    
    TODO
    """

    #  tradier api domain

    #  tradier api version

    #  tradier beta version

    #  tradier bearer token

    #  query symbol count

    #  sector mapping file

    #  data download directory

    #  data date range span
