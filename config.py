"""
Copyright: Travis Cammack 2017
Original Author: Travis Cammack
Create Date: 5/6/2017
Contributors:
################################################################################

This module holds some sweet action settings
"""

import json

#  These are the global settings that will be populated from a config file
TRADIER_API_DOMAIN = None
TRADIER_API_VERSION = None
TRADIER_BETA_VERSION = None
TRADIER_BEARER_TOKEN = None
QUERY_SYMBOL_COUNT = None
START_DATE = None
DB_NAME = None
DB_USER = None
DB_PASSWORD = None
TZ = None  # EST since NASDAQ and NYSE are in New York


def load(file_name):
    """Load the configuration file

    Load in the configuration variable values from a file
    """

    config = None
    with open(file_name, 'r') as f:
        config = json.load(f)

    global TRADIER_API_DOMAIN
    global TRADIER_API_VERSION
    global TRADIER_BETA_VERSION
    global TRADIER_BEARER_TOKEN
    global QUERY_SYMBOL_COUNT
    global START_DATE
    global DB_NAME
    global DB_USER
    global DB_PASSWORD
    global TZ

    TRADIER_API_DOMAIN = config['tradier_api_domain']
    TRADIER_API_VERSION = config['tradier_api_version']
    TRADIER_BETA_VERSION = config['tradier_beta_version']
    TRADIER_BEARER_TOKEN = config['tradier_bearer_token']
    QUERY_SYMBOL_COUNT = config['query_symbol_count']
    START_DATE = config['start_date']
    DB_NAME = config['db_name']
    DB_USER = config['db_user']
    DB_PASSWORD = config['db_password']
    TZ = config['tz']


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
