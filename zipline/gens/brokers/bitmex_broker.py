import os
import uuid
from collections import defaultdict, OrderedDict

import bitmex
import numpy as np
import pandas as pd
from bitmex_websocket import BitMEXWebsocket

import zipline.protocol as zp
from zipline.api import symbol as symbol_lookup
from zipline.errors import SymbolNotFound
from zipline.finance.order import (Order as ZPOrder,
                                   ORDER_STATUS as ZP_ORDER_STATUS)
from zipline.finance.transaction import Transaction
from zipline.gens.brokers.broker import Broker

TEST_WS_ENTRY = 'wss://testnet.bitmex.com/realtime'
WS_ENTRY = 'wss://www.bitmex.com/realtime'


class Account:
    def __init__(self, cash, portfolio_value):
        self.cash = cash
        self.portfolio_value = portfolio_value


"""
api works :

client.Instrument.Instrument_get(filter=json.dumps({'symbol': 'XBTUSD'})).result()

client.Order.Order_new(symbol='XBTUSD', orderQty=10, price=12345.0).result()

client.Order.Order_new(symbol='XBTUSD', orderQty=-10, price=12345.0).result()

client.Order.Order_cancel(orderID='').result()

client.Order.Order_cancelAll().result()

"""


class BITMEXBroker(Broker):

    def __init__(self, uri):
        is_test = os.environ.get('is_test') == 'true'
        self.api_key = os.environ.get('bitmex_api_key')
        self.api_secret = os.environ.get('bitmex_api_secret')
        endpoint = TEST_WS_ENTRY if is_test else WS_ENTRY
        self.ws_client = BitmexWS(endpoint=endpoint,
                                  symbol='XBTUSD',
                                  api_key=self.api_key,
                                  api_secret=self.api_secret)
        self.api_client = bitmex.bitmex(is_test, None, self.api_key, self.api_secret)

    def _new_order_id(self):
        return uuid.uuid4().hex

    def get_stop_price(self, symbol):
        recent_trades = self.ws_client.recent_trades()
        return recent_trades[-1]['price']

    def get_limit_price(self, symbol):
        recent_trades = self.ws_client.recent_trades()
        return recent_trades[-1]['price']

    def _order2zp(self, order):
        zp_order = ZPOrder(
            id=order.client_order_id,
            asset=symbol_lookup(order.symbol),
            amount=int(order.qty) if order.side == 'buy' else -int(order.qty),
            stop=float(order.stop_price) if order.stop_price else None,
            limit=float(order.limit_price) if order.limit_price else None,
            dt=order.submitted_at,
            commission=0,
        )
        zp_order.status = ZP_ORDER_STATUS.OPEN
        if order.canceled_at:
            zp_order.status = ZP_ORDER_STATUS.CANCELLED
        if order.failed_at:
            zp_order.status = ZP_ORDER_STATUS.REJECTED
        if order.filled_at:
            zp_order.status = ZP_ORDER_STATUS.FILLED
            zp_order.filled = int(order.filled_qty)
        return zp_order

    def order(self, asset, amount, style):
        symbol = asset.symbol
        qty = amount if amount > 0 else -amount
        side = 'buy' if amount > 0 else 'sell'
        order_type = 'market'

        zp_order_id = self._new_order_id()
        dt = pd.to_datetime('now', utc=True)
        stop_price = self.get_stop_price(symbol)
        limit_price = self.get_limit_price(symbol)
        zp_order = ZPOrder(
            dt=dt,
            asset=asset,
            amount=amount,
            stop=stop_price,
            limit=limit_price,
            id=zp_order_id,
        )

        order = self.api_client.Order.Order_new(symbol=symbol, orderQty=qty,
                                                price=limit_price).result()[0]
        zp_order = self._order2zp(order)

        return zp_order

    def cancel_order(self, zp_order_id):
        return self.api_client.Order.Order_cancel(orderID=zp_order_id).result()[0]

    def cancel_all(self):
        return self.api_client.Order.Order_cancelAll().result()[0]

    def get_last_traded_dt(self, asset):
        pass

    def subscribe_to_market_data(self, asset):
        pass

    @property
    def subscribed_assets(self):
        return []

    @property
    def positions(self):
        z_positions = zp.Positions()

        positions = self.api_client.Position.Position_get().result()[0][0]
        position_map = {}
        symbols = []
        for pos in positions:
            symbol = pos['symbol']
            try:
                z_position = zp.Position(symbol_lookup(symbol))
            except SymbolNotFound:
                continue
            z_position.amount = pos['currentQty']
            z_position.cost_basis = float(pos['currentCost'])
            z_position.last_sale_price = None
            z_position.last_sale_date = None
            z_positions[symbol_lookup(symbol)] = z_position
            symbols.append(symbol)
            position_map[symbol] = z_position

        quotes = self.ws_client.recent_trades()
        for quote in quotes:
            if quote['symbol'] != 'XBTUSD':
                continue
            price = quote['price']
            dt = quote['timestamp']
            z_position = position_map[quote['symbol']]
            z_position.last_sale_price = float(price)
            z_position.last_sale_date = dt
        return z_positions

    @property
    def portfolio(self):
        account = self.api_client.Position.Position_get().result()
        z_portfolio = zp.Portfolio()
        z_portfolio.cash = float(account['cash'])
        z_portfolio.positions = self.positions
        z_portfolio.positions_value = float(
            account.portfolio_value) - float(account.cash)
        z_portfolio.portfolio_value = float(account.portfolio_value)
        return z_portfolio

    @property
    def account(self):
        account = self.api_client.Position.Position_get().result()
        z_account = zp.Account()
        z_account.buying_power = float(account['cash'])
        z_account.total_position_value = float(
            account.portfolio_value) - float(account['cash'])
        return z_account

    @property
    def time_skew(self):
        return pd.Timedelta('0 sec')

    @property
    def orders(self):
        # todo : make it work
        return {
            o.client_order_id: self._order2zp(o)
            for o in self.api_client.list_orders('all')
        }

    @property
    def transactions(self):
        # todo : make it work
        orders = self.api_client.list_orders(status='closed')
        results = {}
        for order in orders:
            if order.filled_at is None:
                continue
            tx = Transaction(
                asset=symbol_lookup(order.symbol),
                amount=int(order.filled_qty),
                dt=order.filled_at,
                price=float(order.filled_avg_price),
                order_id=order.client_order_id,
                commission=0.0,
            )
            results[order.client_order_id] = tx
        return results

    def get_spot_value(self, assets, field, dt, data_frequency):
        assert (field in (
            'open', 'high', 'low', 'close', 'volume', 'price', 'last_traded'))
        assets_is_scalar = not isinstance(assets, (list, set, tuple))
        if assets_is_scalar:
            symbols = [assets.symbol]
        else:
            symbols = [asset.symbol for asset in assets]
        if field in ('price', 'last_traded'):
            quotes = self.api_client.list_quotes(symbols)
            if assets_is_scalar:
                if field == 'price':
                    if len(quotes) == 0:
                        return np.nan
                    return quotes[-1].last
                else:
                    if len(quotes) == 0:
                        return pd.NaT
                    return quotes[-1].last_timestamp
            else:
                return [
                    quote.last if field == 'price' else quote.last_timestamp
                    for quote in quotes
                ]

        bars_list = self.api_client.list_bars(symbols, '1Min', limit=1)
        if assets_is_scalar:
            if len(bars_list) == 0:
                return np.nan
            return bars_list[0].bars[-1]._raw[field]
        bars_map = {a.symbol: a for a in bars_list}
        return [
            bars_map[symbol].bars[-1]._raw[field]
            for symbol in symbols
        ]

    def get_realtime_bars(self, assets, frequency):
        assets_is_scalar = not isinstance(assets, (list, set, tuple))
        is_daily = 'd' in frequency  # 'daily' or '1d'
        if assets_is_scalar:
            symbols = [assets.symbol]
        else:
            symbols = [asset.symbol for asset in assets]
        timeframe = '1D' if is_daily else '1Min'

        bars_list = self.api_client.list_bars(symbols, timeframe, limit=500)
        bars_map = {a.symbol: a for a in bars_list}
        dfs = []
        for asset in assets if not assets_is_scalar else [assets]:
            symbol = asset.symbol
            df = bars_map[symbol].df.copy()
            if df.index.tz is None:
                df.index = df.index.tz_localize('utc').tz_convert('Asia/Shanghai')
            df.columns = pd.MultiIndex.from_product([[asset, ], df.columns])
            dfs.append(df)
        return pd.concat(dfs, axis=1)
