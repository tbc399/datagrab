#!/usr/bin/env python

"""
Copyright: Travis Cammack 2017
Original Author: Travis Cammack
Create Date: 5/6/2017
Contributors:
################################################################################

This is the driver for datagrab
"""


from dateutil import tz, parser
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
import multiprocessing


@click.group()
def cli():
    pass


@click.command()
@click.option("-f", "--frequency", type=click.Choice(["daily"], case_sensitive=False))
def datefix(frequency):

    cwd = pathlib.Path().home() / f".zipline/csv/tiingo/{frequency}_/clean"

    files = cwd.glob("*.csv")
    # files = cwd.glob('SPY.csv')

    for file in files:
        print(f"Fixing {file.name}")
        with open(file, "r") as f:
            reader = csv.reader(f)
            new_lines = [line for line in reader]
            for line in new_lines[1:]:
                line[0] = line[0].split("T")[0]
        with open(file, "w") as f:
            writer = csv.writer(f)
            writer.writerows(new_lines)


@click.command()
def minmax_dates():

    cwd = pathlib.Path().cwd() / "daily"

    files = cwd.glob("*.csv")

    dates = list()

    for file in files:
        with open(file, "r") as f:
            reader = csv.reader(f)
            new_lines = [line for line in reader]
            for line in new_lines[1:]:
                dates.append(datetime.datetime.strptime(line[0], "%Y-%m-%d").date())

    dates.sort()

    print(dates[0])
    print(dates[-1])


def _worker(args):
    file, frequency = args

    clean_dir = pathlib.Path().home() / f".zipline/csv/tiingo/{frequency}_/clean"

    with open(file, "r") as f:
        reader = csv.reader(f)
        headers = next(reader)  # skip header
        lines = list(reader)

    if not lines:
        print(f"No raw data in {file.name}")
        return

    start_date = parser.isoparse(lines[0][0]).replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=None
    )
    end_date = parser.isoparse(lines[-1][0]).replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=None
    )

    if start_date == end_date:
        print(f"No data for {file.name}")
        return

    calendar = exchange_calendar_xnys.XNYSExchangeCalendar(start_date, end_date)
    sessions = calendar.sessions_in_range(start_date, end_date)

    clean_lines = [
        line
        for line in lines
        if parser.isoparse(line[0]).replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None
        )
        in sessions
    ]

    if len(sessions) > len(clean_lines):
        print(f"Missing sessions in {file.name}")
        return

    if not len(clean_lines):
        print(f"No clean data for {file.name}")
        return

    with open(clean_dir / file.name, "w") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(clean_lines)

    print(f"Cleaned {file.name}")


@click.command()
@click.option(
    "-f",
    "--frequency",
    type=click.Choice(["daily", "5min", "30min"], case_sensitive=False),
)
@click.option("-s", "--start", type=click.DateTime(formats=["%Y-%m-%d"]))
def date_clean(frequency, start):

    raw_dir = pathlib.Path().home() / f".zipline/csv/tiingo/{frequency}_/raw"
    clean_dir = pathlib.Path().home() / f".zipline/csv/tiingo/{frequency}_/clean"

    if not clean_dir.exists():
        clean_dir.mkdir(parents=True)

    files = [(file, frequency) for file in sorted(raw_dir.glob("*.csv"))]

    with multiprocessing.Pool(7) as pool:
        print("start")
        pool.map(_worker, files, chunksize=20)
        print("finish")


@click.command()
@click.option("--compressed", is_flag=True)
@click.option(
    "-f",
    "--frequency",
    type=click.Choice(["daily", "5min", "30min"], case_sensitive=False),
)
def tiingo_json_to_csv(compressed, frequency):

    json_dir = pathlib.Path.home() / f".zipline/json/tiingo/{frequency}_"
    clean_dir = pathlib.Path().home() / f".zipline/csv/tiingo/{frequency}_/raw"

    if not clean_dir.exists():
        clean_dir.mkdir(parents=True)

    extension = "*.json"
    open_func = open
    open_mode = "r"
    if compressed:
        extension += ".bz2"
        open_func = bz2.open
        open_mode = "rt"

    files = json_dir.glob(extension)

    for i, file in enumerate(files):
        print(f"Converting {file.name}")
        with open_func(file, open_mode) as f:
            try:
                json_string = json.load(f)
            except json.decoder.JSONDecodeError as e:
                print(f"Error decoding json file {file.name}")
            with open(clean_dir / f"{file.name.split('.')[0]}.csv", "w") as g:
                csv_writer = csv.writer(g)
                csv_writer.writerow(
                    (
                        "date",
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "dividend",
                        "split",
                    )
                )
                for obj in json_string:
                    csv_writer.writerow(
                        (
                            obj["date"],
                            obj["open"],
                            obj["high"],
                            obj["low"],
                            obj["close"],
                            obj["volume"],
                            obj.get("divCash", 0.0),
                            obj.get("splitFactor", 1.0),
                        )
                    )


@click.command()
@click.option("-i", "--interval", type=click.Choice(["1d", "5m"]))
def yahoo_clean(interval):
    """Clean up the yahoo csv format to match what Zipline is looking for"""
    raw_dir = pathlib.Path.home() / f".zipline/csv/yahoo/{interval}/raw"
    clean_dir = pathlib.Path().home() / f".zipline/csv/yahoo/{interval}/clean"

    if not clean_dir.exists():
        clean_dir.mkdir(parents=True)

    files = raw_dir.glob("*.csv")

    for i, file in enumerate(files):
        with open(file, "r") as f:
            reader = csv.DictReader(f)
            with open(clean_dir / file.name, "w") as g:
                headers = (
                    "date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "dividend",
                    "split",
                )
                writer = csv.DictWriter(g, fieldnames=headers)
                writer.writeheader()
                for row in reader:
                    writer.writerow(
                        {
                            "date": str(
                                parser.parse(row["Datetime"])
                                .replace(tzinfo=tz.gettz("EST"))
                                .astimezone(tz.UTC)
                            )
                            if interval != "1d"
                            else row["Date"],
                            "open": round(float(row["Open"]), 5)
                            if row["Open"]
                            else float("nan"),
                            "high": round(float(row["High"]), 5)
                            if row["High"]
                            else float("nan"),
                            "low": round(float(row["Low"]), 5)
                            if row["Low"]
                            else float("nan"),
                            "close": round(float(row["Close"]), 5)
                            if row["Close"]
                            else float("nan"),
                            "volume": row["Volume"] if row["Volume"] else float("nan"),
                            "dividend": 0.0,
                            "split": 1.0,
                        }
                    )


cli.add_command(symbols.tiingo_symbols)
cli.add_command(history.iex_history)
cli.add_command(history.tiingo_history)  # step 1
cli.add_command(datefix)  # step 4
cli.add_command(minmax_dates)  # optional to get the earliest and latest dates
cli.add_command(date_clean)  # step 3
cli.add_command(tiingo_json_to_csv)  # step 2

# Yahoo Data
cli.add_command(history.yahoo_history)  # step 1
cli.add_command(yahoo_clean)  # step 2

# NOTES
# TODO: Have to manually add 'US' as the exchange country after bundle is ingested
# TODO:


if __name__ == "__main__":
    cli()
