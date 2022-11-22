import click
import yfinance
import pathlib
import tiingo
import csv
import datetime
import json


@click.command()
def iex_history():

    zipline_dir = pathlib.Path.home() / '.zipline/'
    symbols_dir = zipline_dir / 'symbols/'
    daily_csv_dir = zipline_dir / 'csv/iex/daily'

    with open(symbols_dir / 'iex.csv', 'r') as f:
        csv_reader = csv.reader(f)
        symbols = [row[0] for row in csv_reader]

    for symbol in symbols:

        historical_prices = get_historical_data(
            symbol,
            start='20070101',
            end=datetime.date.today(),
            output_format='json',
            token='Tpk_c379f68921904f84966f5f0b275a278f',
        )

        if not daily_csv_dir.exists():
            daily_csv_dir.mkdir(parents=True)

        for symbol_, data in historical_prices.items():
            with open(daily_csv_dir / f'{symbol_}.csv', 'w') as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        'date',
                        'open',
                        'high',
                        'low',
                        'close',
                        'volume',
                        'dividend',
                        'split',
                    ]
                )
                for prices in data['chart']:
                    try:
                        writer.writerow(
                            (
                                prices['date'],
                                prices['fOpen']
                                if 'fOpen' in prices
                                else prices['open'],
                                prices['fHigh']
                                if 'fHigh' in prices
                                else prices['high'],
                                prices['fLow'] if 'fLow' in prices else prices['low'],
                                prices['fClose']
                                if 'fClose' in prices
                                else prices['close'],
                                prices['fVolume']
                                if 'fVolume' in prices
                                else prices['volume'],
                                0.0,
                                1.0,
                            )
                        )
                    except KeyError as error:
                        click.echo(
                            f'field {error} missing in {symbol_} at {prices["date"]}'
                        )
                        pass

        click.echo(f'finished symbol {symbol}')


@click.command()
@click.option(
    '-f',
    '--frequency',
    type=click.Choice(
        ['daily', '5min'],
        case_sensitive=False,
    ),
)
def tiingo_history(frequency):

    zipline_dir = pathlib.Path.home() / '.zipline/'
    symbols_dir = zipline_dir / 'symbols/'

    json_dir = zipline_dir / f'json/tiingo/{frequency}'

    with open(symbols_dir / 'tiingo.csv', 'r') as f:
        csv_reader = csv.reader(f)
        symbols = [row for row in csv_reader]

    client = tiingo.TiingoClient(
        config={'api_key': '6401083a570395b73daa90d694e19a07bf9920e7', 'session': True}
    )

    for symbol, start_date, end_date in symbols:

        try:
            historical_prices = client.get_ticker_price(
                symbol, startDate=start_date, endDate=end_date, frequency=frequency
            )
        except Exception as e:
            click.echo(f'Failed for symbol {symbol}')
            continue

        if not json_dir.exists():
            json_dir.mkdir(parents=True)

        with open(json_dir / f'{symbol}.json', 'w') as f:
            json.dump(historical_prices, f)

        click.echo(f'finished symbol {symbol}')


@click.command()
@click.option('-i', '--interval', type=click.Choice(['5m'], case_sensitive=True))
def yahoo_history(interval):

    zipline_dir = pathlib.Path.home() / '.zipline/'
    symbols_dir = zipline_dir / 'symbols/'

    json_dir = zipline_dir / f'csv/yahoo/{interval}/raw/'

    if not json_dir.exists():
        json_dir.mkdir(parents=True)

    with open(symbols_dir / 'tiingo.csv', 'r') as f:
        csv_reader = csv.reader(f)
        symbols = [row for row in csv_reader]

    for symbol, _, end_date in symbols[:10]:
        data = yfinance.download(
            symbol,
            period='60d',
            interval=interval,
            ignore_tz=False,
            auto_adjust=True,
        )
        if not data.empty:
            data.to_csv(json_dir / f'{symbol}.csv')
            click.echo(f'finished symbol {symbol}')
