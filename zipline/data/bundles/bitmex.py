import logging
from datetime import time, timedelta

from pytz import timezone
import numpy as np
import pandas as pd
from pandas.tseries.offsets import CustomBusinessDay
from zipline.utils.calendars import (
    TradingCalendar, register_calendar, register_calendar_alias,
    deregister_calendar)
from trading_calendars.always_open import AlwaysOpenCalendar

from zipline.data.bundles import register
from zipline.utils.memoize import lazyval

from data.zipline_ingest.crypto.bitmex_data_api import get_currencies, get_trade_hist_alias
_logger = logging.getLogger(__name__)

import traceback
import time as origin_time

from functools import wraps



def retry_call(n):
    def single_retry(func):
        @wraps(func)
        def wrap_func(*args, **kwargs):
            for i in range(n):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    detail = traceback.format_exc() if n == 0 else ''
                    print("[error retry] {} {}".format(e, detail))
                    origin_time.sleep(n)
            raise Exception('[retry giveup]')
        return wrap_func
    return single_retry


class Pairs(object):
    """Record object holding most common US-$ / crypto-currency pairs
    """
    symbol_btc = 'XBTUSD'
    symbol_ada = 'ADAZ18'
    symbol_bch = 'BCHZ18'
    symbol_eos = 'EOSZ18'
    symbol_eth = 'ETHUSD'
    symbol_ltc = 'LTCZ18'
    symbol_trx = 'TRXZ18'
    symbol_xrp = 'XRPZ18'


def fetch_assets(asset_pairs):
    return pd.DataFrame([{'symbol': i, 'asset_name': i[:3], 'exchange': 'bitmex'} for i in asset_pairs])


def fetch_assets_origin(asset_pairs):
    """Fetch given asset pairs

    Args:
        asset_pairs (list): list of asset pairs

    Returns:
        pandas.DataFrame: dataframe of asset pairs
    """
    asset_pair_map = {pair[:3]: pair for pair in asset_pairs}
    all_assets = get_currencies()
    asset_df = all_assets.ix[asset_pair_map.keys()].reset_index()
    asset_df = asset_df[['index', 'name']].rename(
        columns={'index': 'symbol', 'name': 'asset_name'})
    asset_df['exchange'] = 'Bitmex'  # needed despite documented as optional
    return asset_df


def make_candle_stick(trades, freq='1Min'):
    volume = trades['amount'].resample(freq).sum()
    volume = volume.fillna(0)
    high = trades['rate'].resample(freq).max()
    low = trades['rate'].resample(freq).min()
    open = trades['rate'].resample(freq).first()
    close = trades['rate'].resample(freq).last()
    return pd.DataFrame(
        dict(open=open, high=high, low=low, close=close, volume=volume))


@retry_call(2)
def fetch_trades(asset_pair, start, end):
    """Helper function to fetch trades for a single asset pair

    Does all necessary conversions, sets `date` as index and assures
    that `start` and `end` are in the index.

    Args:
        asset_pair: name of the asset pair
        start (pandas.Timestamp): start of period
        end (pandas.Timestamp): end of period

    Returns:
        pandas.DataFrame: dataframe containing trades of asset
    """

    df = get_trade_hist_alias(asset_pair, start, end).rename(columns={
        'vol': 'amount',
        'price': 'rate',
    })
    df['total'] = df['amount'] * df['rate']
    df['date'] = df['date'].apply(lambda x: x.replace(tzinfo=timezone('UTC')))

    for col in ('total', 'rate', 'amount'):
        df[col] = df[col].astype(np.float32)
    df = df.set_index('date')
    if start not in df.index:
        df.loc[start] = np.nan
    if end not in df.index:
        df.loc[end] = np.nan
    return df


def prepare_data(start, end, sid_map, cache):
    """Retrieve and prepare trade data for ingestion

    Args:
        start (pandas.Timestamp): start of period
        end (pandas.Timestamp): end of period
        sid_map (dict): mapping from symbol id to asset pair name
        cache: cache object as provided by zipline

    Returns:
        generator of symbol id and dataframe tuples
    """
    def get_key(sid, day):
        return "{}_{}".format(sid, day.strftime("%Y-%m-%d %H:%M:%S"))

    for sid, asset_pair in sid_map.items():

        for start_day in pd.date_range(start, end, freq='D', closed='left', tz='utc'):
            print("\nprocessing ", start_day, asset_pair)
            key = get_key(sid, start_day)

            if key not in cache:
                end_day = start_day + timedelta(days=1, seconds=-1)

                trades = fetch_trades(asset_pair, start_day, end_day)
                cache[key] = make_candle_stick(trades)
                _logger.debug("Fetched trades from {} to {}".format(start_day, end_day))
            yield sid, cache[key]


def create_bundle(asset_pairs, start=None, end=None):
    """Create a bundle ingest function

    Args:
        asset_pairs (list): list of asset pairs
        start (pandas.Timestamp): start of trading period
        end (pandas.Timestamp): end of trading period

    Returns:
        ingest function needed by zipline's register.
    """
    def ingest(environ,
               asset_db_writer,
               minute_bar_writer,
               daily_bar_writer,
               adjustment_writer,
               calendar,
               start_session,
               end_session,
               cache,
               show_progress,
               output_dir,
               # pass these as defaults to make them 'nonlocal' in py2
               start=start,
               end=end):

        if start is None:
            start = start_session
        if end is None:
            end = end_session

        adjustment_writer.write()
        asset_df = fetch_assets(asset_pairs)
        asset_db_writer.write(equities=asset_df)
        # generate the mapping between sid and symbol name
        asset_map = asset_df['symbol'].to_dict()
        asset_pair_map = {pair: pair[:3] for pair in asset_pairs}
        sid_map = {k: asset_pair_map[v] for k, v in asset_map.items()}

        data = prepare_data(start, end, sid_map, cache)

        minute_bar_writer.write(data, show_progress=show_progress)
    return ingest


register_calendar('BITMEX', AlwaysOpenCalendar())
# The following is necessary because zipline's developer hard-coded NYSE
# everywhere in run_algo._run, *DOH*!!!
deregister_calendar('NYSE')
register_calendar_alias('NYSE', 'BITMEX', force=False)
register(
    'bitmex',
    create_bundle(
        [
            Pairs.symbol_btc,
            #Pairs.symbol_ada,
            #Pairs.symbol_bch,
            #Pairs.symbol_eos,
            #Pairs.symbol_eth,
            #Pairs.symbol_ltc,
            #Pairs.symbol_trx,
            #Pairs.symbol_xrp,
        ],
        pd.Timestamp('2017-11-05 00:00:00', tz='utc'),
        pd.Timestamp('2018-12-18 23:59:59', tz='utc'),
    ),
    calendar_name='BITMEX',
    minutes_per_day=24*60
)


