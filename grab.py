#!/usr/bin/env python

'''
Copyright: Travis Cammack 2017
Original Author: Travis Cammack
Create Date: 5/6/2017
Contributors:
################################################################################

This is the driver for datagrab
'''


import bz2
import json

import click

import symbols
import pandas
import pytz
import history
import pathlib
import shutil
import csv
import datetime
from exchange_calendars import exchange_calendar_xnys


@click.group()
def cli():
    pass


@click.command()
def datefix():

    cwd = pathlib.Path().home() / '.zipline/csv/tiingo/daily'

    # files = cwd.glob('*.csv')
    files = cwd.glob('SPY.csv')

    for file in files:
        with open(file, 'r') as f:
            reader = csv.reader(f)
            new_lines = [line for line in reader]
            for line in new_lines[1:]:
                line[0] = line[0].split('T')[0]
        with open(file, 'w') as f:
            writer = csv.writer(f)
            writer.writerows(new_lines)


@click.command()
def minmax_dates():

    cwd = pathlib.Path().cwd() / 'daily'

    files = cwd.glob('*.csv')

    dates = list()

    for file in files:
        with open(file, 'r') as f:
            reader = csv.reader(f)
            new_lines = [line for line in reader]
            for line in new_lines[1:]:
                dates.append(datetime.datetime.strptime(line[0], '%Y-%m-%d').date())

    dates.sort()

    print(dates[0])
    print(dates[-1])


@click.command()
def date_clean():

    raw_dir = pathlib.Path().home() / '.zipline/csv/tiingo/raw-daily'
    clean_dir = pathlib.Path().home() / '.zipline/csv/tiingo/daily'

    if not clean_dir.exists():
        clean_dir.mkdir(parents=True)

    # files = raw_dir.glob('*.csv')
    files = raw_dir.glob('SPY.csv')

    for file in files:
        with open(file, 'r') as f:
            reader = csv.reader(f)
            lines = [line for line in reader]
            dates = [pandas.Timestamp(line[0], tz=pytz.UTC) for line in lines[1:]]
            if len(dates) == 0:
                continue
            calendar = exchange_calendar_xnys.XNYSExchangeCalendar(
                start=dates[0], end=dates[-1]
            )
            calendar_sessions = calendar.sessions_in_range(dates[0], dates[-1])
            if len(dates) == len(calendar_sessions) and all(
                [x == y for x, y in zip(dates, calendar_sessions)]
            ):
                shutil.copy(file, clean_dir)
            else:
                print(f'skipping {file.name}')


@click.command()
@click.option('--compressed', is_flag=True)
@click.option(
    '-f', '--frequency', type=click.Choice(['daily', '5min'], case_sensitive=False)
)
def tiingo_json_to_csv(compressed, frequency):

    json_dir = pathlib.Path.home() / f'.zipline/json/tiingo/{frequency}'
    clean_dir = pathlib.Path().home() / f'.zipline/csv/tiingo/raw-{frequency}'

    extension = '*.json'
    open_func = open
    open_mode = 'r'
    if compressed:
        extension += '.bz2'
        open_func = bz2.open
        open_mode = 'rt'

    files = json_dir.glob(extension)
    # files = json_dir.glob('SPY.json')

    for i, file in enumerate(files):
        with open_func(file, open_mode) as f:
            json_string = json.load(f)
            with open(clean_dir / f"{file.name.split('.')[0]}.csv", 'w') as g:
                csv_writer = csv.writer(g)
                csv_writer.writerow(
                    (
                        'date',
                        'open',
                        'high',
                        'low',
                        'close',
                        'volume',
                        'dividend',
                        'split',
                    )
                )
                for obj in json_string:
                    csv_writer.writerow(
                        (
                            obj['date'],
                            obj['open'],
                            obj['high'],
                            obj['low'],
                            obj['close'],
                            obj['volume'],
                            obj['divCash'],
                            obj['splitFactor'],
                        )
                    )


@click.command()
@click.option('-i', '--interval', type=click.Choice(['5m']))
def yahoo_clean(interval):
    """Clean up the yahoo csv format to match what Zipline is looking for"""
    raw_dir = pathlib.Path.home() / f'.zipline/csv/yahoo/{interval}/raw'
    clean_dir = pathlib.Path().home() / f'.zipline/csv/yahoo/{interval}/clean'

    if not clean_dir.exists():
        clean_dir.mkdir(parents=True)

    files = raw_dir.glob('*.csv')

    for i, file in enumerate(files):
        with open(file, 'r') as f:
            reader = csv.DictReader(f)
            with open(clean_dir / file.name, 'w') as g:
                headers = (
                    'date',
                    'open',
                    'high',
                    'low',
                    'close',
                    'volume',
                    'dividend',
                    'split',
                )
                writer = csv.DictWriter(g, fieldnames=headers)
                writer.writeheader()
                for row in reader:
                    writer.writerow(
                        {
                            'date': row['Datetime'],
                            'open': round(float(row['Open']), 5),
                            'high': round(float(row['High']), 5),
                            'low': round(float(row['Low']), 5),
                            'close': round(float(row['Close']), 5),
                            'volume': row['Volume'],
                            'dividend': 0.0,
                            'split': 1.0,
                        }
                    )


cli.add_command(symbols.tiingo_symbols)
cli.add_command(history.iex_history)
cli.add_command(history.tiingo_history)
cli.add_command(datefix)
cli.add_command(minmax_dates)
cli.add_command(date_clean)
cli.add_command(tiingo_json_to_csv)

# Yahoo Data
cli.add_command(history.yahoo_history)  # step 1
cli.add_command(yahoo_clean)  # step 2


if __name__ == '__main__':
    cli()
