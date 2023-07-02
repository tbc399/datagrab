import click
from utils import asink
import aiofile
import toolz
import asyncio
import httpx
import time
import yfinance
import pathlib
import tiingo
import csv
import datetime
import json
from tenacity import AsyncRetrying, stop_after_attempt, RetryError


def get_ticker_price(
    self, ticker, startDate=None, endDate=None, fmt="json", frequency="daily", **extra
):
    """By default, return latest EOD Composite Price for a stock ticker.
    On average, each feed contains 3 data sources.

     Supported tickers + Available Day Ranges are here:
     https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip

     Args:
         ticker (string): Unique identifier for stock ticker
         startDate (string): Start of ticker range in YYYY-MM-DD format
         endDate (string): End of ticker range in YYYY-MM-DD format
         fmt (string): 'csv' or 'json'
         frequency (string): Resample frequency
    """
    url = self._get_url(ticker, frequency)
    params = {
        "format": fmt if fmt != "object" else "json",  # conversion local
        "resampleFreq": frequency,
    }

    if extra:
        if not self._is_eod_frequency(frequency):
            if "columns" in extra:
                params["columns"] = extra["columns"]

    if startDate:
        params["startDate"] = startDate
    if endDate:
        params["endDate"] = endDate

    # TODO: evaluate whether to stream CSV to cache on disk, or
    # load as array in memory, or just pass plain text
    response = self._request("GET", url, params=params)
    if fmt == "json":
        return response.json()
    elif fmt == "object":
        data = response.json()
        return [tiingo.api.dict_to_object(item, "TickerPrice") for item in data]
    else:
        return response.content.decode("utf-8")


setattr(tiingo.TiingoClient, "get_ticker_price", get_ticker_price)


@click.command()
def iex_history():

    zipline_dir = pathlib.Path.home() / ".zipline/"
    symbols_dir = zipline_dir / "symbols/"
    daily_csv_dir = zipline_dir / "csv/iex/daily"

    with open(symbols_dir / "iex.csv", "r") as f:
        csv_reader = csv.reader(f)
        symbols = [row[0] for row in csv_reader]

    for symbol in symbols:

        historical_prices = get_historical_data(
            symbol,
            start="20070101",
            end=datetime.date.today(),
            output_format="json",
            token="Tpk_c379f68921904f84966f5f0b275a278f",
        )

        if not daily_csv_dir.exists():
            daily_csv_dir.mkdir(parents=True)

        for symbol_, data in historical_prices.items():
            with open(daily_csv_dir / f"{symbol_}.csv", "w") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "date",
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "dividend",
                        "split",
                    ]
                )
                for prices in data["chart"]:
                    try:
                        writer.writerow(
                            (
                                prices["date"],
                                prices["fOpen"]
                                if "fOpen" in prices
                                else prices["open"],
                                prices["fHigh"]
                                if "fHigh" in prices
                                else prices["high"],
                                prices["fLow"] if "fLow" in prices else prices["low"],
                                prices["fClose"]
                                if "fClose" in prices
                                else prices["close"],
                                prices["fVolume"]
                                if "fVolume" in prices
                                else prices["volume"],
                                0.0,
                                1.0,
                            )
                        )
                    except KeyError as error:
                        click.echo(
                            f'field {error} missing in {symbol_} at {prices["date"]}'
                        )
                        pass

        click.echo(f"finished symbol {symbol}")


@click.command()
@click.option(
    "-f",
    "--frequency",
    type=click.Choice(
        ["daily", "5min", "30min"],
        case_sensitive=False,
    ),
)
@asink
async def tiingo_history(frequency):

    zipline_dir = pathlib.Path.home() / ".zipline/"
    symbols_dir = zipline_dir / "symbols/"

    csv_dir = zipline_dir / f"csv/tiingo/{frequency}_/raw"

    if not csv_dir.exists():
        csv_dir.mkdir(parents=True)

    with open(symbols_dir / "tiingo.csv", "r") as f:
        csv_reader = csv.reader(f)
        symbols = [row for row in csv_reader]

    tiingo_key = "6401083a570395b73daa90d694e19a07bf9920e7"
    # client = tiingo.TiingoClient(
    #     config={'api_key': '6401083a570395b73daa90d694e19a07bf9920e7', 'session': True}
    # )

    extras = dict(columns=",".join(["date", "close", "high", "low", "open", "volume"]))
    if "min" in frequency:
        path_base = "iex"
        extras.update({"resampleFreq": frequency})
    else:
        path_base = "tiingo/daily"
        extras["columns"] + ",divCash,splitFactor"

    failed_symbols = []

    async def get_price(symbol, start, end):
        async with httpx.AsyncClient() as client:
            try:
                async for attempt in AsyncRetrying(stop=stop_after_attempt(5)):
                    with attempt:
                        resp = await client.get(
                            f"https://api.tiingo.com/{path_base}/{symbol}/prices",
                            params={
                                "startDate": str(start),
                                "endDate": str(end),
                                "format": "csv",
                                **extras,
                            },
                            headers={"Authorization": f"Token {tiingo_key}"},
                            timeout=5,
                        )
                        if resp.status_code == httpx.codes.TOO_MANY_REQUESTS:
                            click.echo("Too many requests")
                            return
                        historical_prices = resp.json()
            except RetryError:
                click.echo(f"Failed to get minute prices for {symbol}")
                failed_symbols.append((symbol, start, end))
                return

        async with aiofile.async_open(csv_dir / f"{symbol}.csv", "w") as f:
            reader = csv.DictReader(f)
            csv_string = json.dumps(historical_prices)
            await f.write(json_string)

        click.echo(f"finished symbol {symbol}")

    chunked_tasks = toolz.partition(
        50, [get_price(symbol, start, end) for symbol, start, end in symbols]
    )
    for chunk in chunked_tasks:
        await asyncio.gather(*chunk)

    if failed_symbols:
        click.echo(
            f"Gonna take a second shot at the {len(failed_symbols)} symbols that failed to download"
        )
        await asyncio.sleep(5)
        # circle back around and try the failures one more time
        chunked_tasks = toolz.partition(
            50,
            [get_price(symbol, start, end) for symbol, start, end in failed_symbols],
        )
        for chunk in chunked_tasks:
            await asyncio.gather(*chunk)
            await asyncio.sleep(1)


@click.command()
@click.option("-i", "--interval", type=click.Choice(["1d", "5m"], case_sensitive=True))
def yahoo_history(interval):

    zipline_dir = pathlib.Path.home() / ".zipline/"
    symbols_dir = zipline_dir / "symbols/"

    json_dir = zipline_dir / f"csv/yahoo/{interval}/raw/"

    if not json_dir.exists():
        json_dir.mkdir(parents=True)

    chunk_size = 200
    with open(symbols_dir / "tiingo.csv", "r") as f:
        csv_reader = csv.reader(f)
        symbols = [symbol for symbol, _, _ in csv_reader]
        break_index = symbols.index("SNDA")
        symbols = symbols[break_index:]
        chunked_symbols = [
            symbols[i : i + chunk_size] for i in range(0, len(symbols), chunk_size)
        ]

    for symbols in chunked_symbols:
        print(symbols)
        data = yfinance.download(
            ",".join(symbols),
            start=(datetime.datetime.now() - datetime.timedelta(days=59)).date(),
            end=(datetime.datetime.now() - datetime.timedelta(days=1)).date(),
            interval=interval,
            ignore_tz=True,
            auto_adjust=True,
            group_by="Ticker",
        )
        for symbol in symbols:
            df = data.get(symbol)
            if not df.empty:
                df.to_csv(json_dir / f"{symbol}.csv")
                click.echo(f"finished symbol {symbol}")
        time.sleep(30)
