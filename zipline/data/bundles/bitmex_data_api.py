import gzip
from datetime import datetime
from io import BytesIO
from io import StringIO

import dateutil.parser
import pandas as pd
import requests
from pytz import timezone


global_cache = {}


class TradesExceeded(Exception):
    pass


class RequestError(Exception):
    pass


def unix_time(dt):
    """Convert datetime to seconds since epoch
    Args:
        dt: datetime object
    Returns:
        seconds since epoch
    """
    epoch = datetime.utcfromtimestamp(0).replace(tzinfo=timezone('UTC'))
    dt = dt.replace(tzinfo=timezone('UTC'))
    return (dt - epoch).total_seconds()


def get_currencies():
    return pd.DataFrame().transpose()


def get_trade_hist_alias(pair, start, end):
    date_str = start.strftime('%Y%m%d')
    if date_str in global_cache:
        df = global_cache[date_str]
        return df.loc[(df.index >= start) & (df.index <= end)]

    url = 'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/{}.csv.gz'.format(date_str)
    raw_data = BytesIO(requests.get(url, stream=True).raw.read()).read()
    decompressed_data = gzip.decompress(raw_data)
    df = pd.read_csv(StringIO(decompressed_data.decode('utf8')))
    df['vol'] = df['homeNotional'] + df['foreignNotional']
    df['date'] = df['timestamp'].apply(lambda x: dateutil.parser.parse(x.replace('D', ' ')))
    df.set_index('date', inplace=True)
    df['date'] = df.index

    df = df[df['symbol'] == 'XBTUSD'].copy()

    global_cache[date_str] = df
    choose_df = df.loc[(df.index >= start) & (df.index <= end)]
    return choose_df


if __name__ == '__main__':
    get_trade_hist_alias('btc', datetime(2018, 10, 10), datetime(2018, 10, 10))
