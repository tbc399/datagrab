import click
import pathlib
from iexfinance.stocks import get_historical_data
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
                writer.writerow([
                    'date',
                    'open',
                    'high',
                    'low',
                    'close',
                    'volume',
                    'dividend',
                    'split'
                ])
                for prices in data['chart']:
                    try:
                        writer.writerow((
                            prices['date'],
                            prices['fOpen'] if 'fOpen' in prices else prices['open'],
                            prices['fHigh'] if 'fHigh' in prices else prices['high'],
                            prices['fLow'] if 'fLow' in prices else prices['low'],
                            prices['fClose'] if 'fClose' in prices else prices['close'],
                            prices['fVolume'] if 'fVolume' in prices else prices['volume'],
                            0.0,
                            1.0
                        ))
                    except KeyError as error:
                        click.echo(f'field {error} missing in {symbol_} at {prices["date"]}')
                        pass
        
        click.echo(f'finished symbol {symbol}')


@click.command()
def tiingo_history():
    
    zipline_dir = pathlib.Path.home() / '.zipline/'
    symbols_dir = zipline_dir / 'symbols/'
    daily_csv_dir = zipline_dir / 'json/tiingo/daily'
    
    with open(symbols_dir / 'tiingo.csv', 'r') as f:
        csv_reader = csv.reader(f)
        symbols = [row for row in csv_reader]
    
    client = tiingo.TiingoClient(
        config={
            'api_key': '6401083a570395b73daa90d694e19a07bf9920e7',
            'session': True
        }
    )
    
    for symbol, start_date, end_date in symbols:
        
        try:
            historical_prices = client.get_ticker_price(
                symbol,
                startDate=start_date,
                endDate=end_date
            )
        except Exception as e:
            click.echo(f'Failed for symbol {symbol}')
            continue
        
        if not daily_csv_dir.exists():
            daily_csv_dir.mkdir(parents=True)

        with open(daily_csv_dir / f'{symbol}.json', 'w') as f:
            json.dump(historical_prices, f)
        
        click.echo(f'finished symbol {symbol}')
