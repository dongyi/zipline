import glob
import sys
from datetime import time

import datetime
import numpy as np
import pandas as pd
from logbook import Logger, StreamHandler
from pandas import date_range
from pandas.tseries.offsets import CustomBusinessDay
from pytz import timezone
from six import iteritems
from zipline.data.bundles import register
from zipline.utils.calendars import (
    TradingCalendar, register_calendar, register_calendar_alias,
    deregister_calendar)
from zipline.utils.memoize import lazyval

handler = StreamHandler(sys.stdout, format_string=" | {record.message}")
logger = Logger(__name__)
logger.handlers.append(handler)

from data.zipline_ingest.cn_calendar import SHSZExchangeCalendar
from trading_calendars.exchange_calendar_xshg import XSHGExchangeCalendar


def get_meta_df():
    """
    hard code IF IC IH RB
    """

    df_meta = pd.DataFrame([
        {'root_symbol': 'IF', 'multiplier': 13, 'asset_name': 'IF', 'exchange': 'CCFX', 'sector': 'financial_futures',
         'root_symbol_id': 1},
        {'root_symbol': 'IH', 'multiplier': 13, 'asset_name': 'IH', 'exchange': 'CCFX', 'sector': 'financial_futures',
         'root_symbol_id': 2},
        {'root_symbol': 'IC', 'multiplier': 13, 'asset_name': 'IC', 'exchange': 'CCFX', 'sector': 'financial_futures',
         'root_symbol_id': 3},
        {'root_symbol': 'RB', 'multiplier': 10, 'asset_name': 'RB', 'exchange': 'XSGE', 'sector': 'shanghai_futures',
         'root_symbol_id': 4},
    ])
    return df_meta


def load_data(path, pairs):
    """

    file path :

    IH /
        IF1804.CCFX_2018-03-25.csv
        IF1804.CCFX_2018-03-26.csv
        IF1804.CCFX_2018-03-27.csv
        ...
    """
    total_list = []
    for asset in pairs:
        file_list = glob.glob(path + '/{}/*.csv'.format(asset))
        df_dict = {}
        for f in file_list:
            df = pd.DataFrame.from_csv(f)
            if df['open'].isnull().values.all():
                continue
            filename = f.split('/')[-1].split('.')[0]
            symbol = filename
            df['symbol'] = filename
            df['root_symbol'] = filename[:2]
            df['delivery'] = filename[2:6]
            df['exchange'] = 'CCFX'
            df['date'] = df.index
            #df['date'] = df['date'].apply(lambda x: x-datetime.timedelta(seconds=8*3600))
            #df.set_index('date')
            #df['date'] = df.index
            df.rename(columns={'money': 'amount'}, inplace=True)
            df.dropna(inplace=True)
            if symbol not in df_dict:
                df_dict[symbol] = []
            df_dict[symbol].append(df)
        for k, v in df_dict.items():
            big_df = pd.concat(v)
            big_df.sort_index(inplace=True)
            big_df = big_df[~big_df.index.duplicated(keep='first')]

            big_df['expiration_date'] = big_df.index.tolist()[-1]
            big_df.index.name = 'date'
            total_list.append(big_df)
    return pd.concat(total_list)


def gen_asset_metadata(raw_data, show_progress):
    if show_progress:
        logger.info('Generating asset metadata.')

    data = raw_data.groupby(
        by='symbol'
    ).agg(
        {'date': [np.min, np.max]}
    )
    data.reset_index(inplace=True)
    data['start_date'] = data.date.amin
    data['end_date'] = data.date.amax

    data['first_traded'] = data['start_date']
    del data['date']
    data.columns = data.columns.get_level_values(0)
    meta = get_meta_df()

    data['root_symbol'] = [s[:2] for s in data.symbol.unique()]
    data = data.merge(meta, on='root_symbol')

    data['auto_close_date'] = data['end_date']  # + pd.Timedelta(days=1)
    data['notice_date'] = data['auto_close_date']

    data['tick_size'] = 0.0001  # Placeholder for now

    return data


def parse_pricing_and_vol(data,
                          sessions,
                          symbol_map,
                          freq, calendar):
    for asset_id, symbol in iteritems(symbol_map):
        sessions_without_tz = sessions.tz_localize(None)
        if freq == 'D':
            start = sessions_without_tz.min().replace(hour=0, minute=0, second=0)
            end = sessions_without_tz.max().replace(hour=0, minute=0, second=0)
            sessions_without_tz = calendar.sessions_in_range_d(start, end)
            sessions_without_tz = pd.Series(x.replace(hour=16, minute=0, second=0) for x in sessions_without_tz)
            data = data[['open', 'close', 'high', 'low', 'volume', 'amount']].copy()
            #del data['root_symbol']
            #del data['delivery']
            #del data['exchange']
            #del data['expiration_date']
            if symbol == 'IC1809':
                print('aaa')
            flattern_data = data.xs(symbol, level=1).resample('1d').agg({
                'close': 'last',
                'open': 'first',
                'low': 'min',
                'high': 'max',
                'volume': 'sum',
                'amount': 'sum'
            }).dropna()
        else:
            flattern_data = data.xs(symbol, level=1).asfreq(freq).copy()
            flattern_data = data.xs(symbol, level=1).asfreq(freq).copy()
        flattern_data['temp_date'] = flattern_data.index
        flattern_data['temp_date'] = flattern_data['temp_date'].apply(lambda x: x-datetime.timedelta(seconds=8*3600))
        flattern_data.set_index('temp_date', inplace=True)
        asset_data = flattern_data.reindex(sessions_without_tz)
        if np.isnan(asset_data.close).all():
            yield asset_id, asset_data.fillna(0)
        else:
            asset_data.volume.fillna(method='ffill', inplace=True)
            asset_data.close.fillna(method='ffill', inplace=True)
            asset_data.open = asset_data.open.fillna(asset_data.close.shift())
            asset_data.low = asset_data.open.fillna(asset_data.close.shift())
            asset_data.high = asset_data.open.fillna(asset_data.close.shift())
            asset_data.fillna(0.0, inplace=True)

            print(asset_id, symbol, asset_data.index.min(), asset_data.index.max())
            yield asset_id, asset_data


class CN_FUTURE_CALENDAR(XSHGExchangeCalendar):
    """
    Round the clock calendar: 7/7, 24/24
    """

    @property
    def name(self):
        return "CN_FUTURES"

    @property
    def tz(self):
        return timezone("Asia/Shanghai")

    @property
    def regular_holidays(self):
        return []

    @property
    def special_opens(self):
        return []

    def sessions_in_range(self, start_session, last_session):
        origin_sessions = date_range(start_session,
                                     last_session,
                                     freq='T')
        holidays = self.precomputed_holidays
        fixed_range = pd.to_datetime([i for i in origin_sessions if i not in holidays and i.weekday() not in [6, 5]])
        return fixed_range
        #return date_range(start_session, last_session, freq='T')

    def sessions_in_range_d(self, start_session, last_session):
        origin_sessions = date_range(start_session, last_session, freq='D')
        holidays = self.precomputed_holidays
        return pd.to_datetime([i for i in origin_sessions if i not in holidays and i.weekday() not in [6, 5]])
        #return origin_sessions

    @lazyval
    def day(self):
        return CustomBusinessDay(holidays=self.adhoc_holidays,
                                 calendar=self.regular_holidays, weekmask="Mon Tue Wed Thu Fri")


def create_bundle(asset_pairs, start, end):
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
               tframes=None,
               csvdir=None):
        # hard code my path

        raw_data = load_data('/Users/dongyi/work/sif/data/', asset_pairs)
        adjustment_writer.write()
        asset_metadata = gen_asset_metadata(raw_data, False)
        root_symbols = asset_metadata.root_symbol.unique()
        root_symbols = pd.DataFrame(root_symbols, columns=['root_symbol'])
        root_symbols['root_symbol_id'] = root_symbols.index.values

        root_symbols['sector'] = [asset_metadata.loc[asset_metadata['root_symbol'] == rs]['sector'].iloc[0] for rs in
                                  root_symbols.root_symbol.unique()]
        root_symbols['exchange'] = [asset_metadata.loc[asset_metadata['root_symbol'] == rs]['exchange'].iloc[0] for rs
                                    in
                                    root_symbols.root_symbol.unique()]
        root_symbols['description'] = [asset_metadata.loc[asset_metadata['root_symbol'] == rs]['asset_name'].iloc[0] for
                                       rs
                                       in root_symbols.root_symbol.unique()]

        asset_db_writer.write(futures=asset_metadata, root_symbols=root_symbols)

        symbol_map = asset_metadata.symbol
        amend_start = start
        amend_end = end

        sessions = calendar.sessions_in_range(amend_start, amend_end)
        raw_data.set_index(['date', 'symbol'], inplace=True)
        minute_bar_writer.write(
            parse_pricing_and_vol(
                raw_data,
                sessions,
                symbol_map, 'T', calendar
            ),
            show_progress=show_progress
        )

        daily_bar_writer.write(
            parse_pricing_and_vol(
                raw_data,
                sessions,
                symbol_map, 'D', calendar
            ),
            show_progress=show_progress
        )

    return ingest


def main():
    register_calendar('CN_FUTURES', CN_FUTURE_CALENDAR(), force=True)

    deregister_calendar('NYSE')

    register_calendar_alias('NYSE', 'CN_FUTURES', force=True)

    register(
        'CN_FUTURES',
        create_bundle(
            ['IC', 'IH', 'IF', 'RB'],
            pd.Timestamp('2017-09-20 01:31:00', tz='utc'),
            pd.Timestamp('2018-10-31 01:31:00', tz='utc'),
        ),
        calendar_name='CN_FUTURES',
        minutes_per_day=4 * 60 + 90
    )


if __name__ == '__main__':
    main()
