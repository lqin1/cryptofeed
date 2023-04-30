'''
Copyright (C) 2018-2023 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import hmac
import time
from collections import defaultdict
from cryptofeed.symbols import Symbol
import logging
from decimal import Decimal
from typing import Dict, Tuple, Union
from datetime import datetime as dt

from yapic import json

from cryptofeed.connection import AsyncConnection, RestEndpoint, Routes, WebsocketEndpoint
from cryptofeed.defines import BID, ASK, MEXC, L2_BOOK, ORDER_INFO, SPOT
from cryptofeed.feed import Feed
from cryptofeed.types import OrderBook


LOG = logging.getLogger('feedhandler')


class Mexc(Feed):
    id = MEXC
    websocket_channels = {
        L2_BOOK: 'spot@public.limit.depth.v3.api@{}@10',
        ORDER_INFO: 'order'
    }
    websocket_endpoints = [
        WebsocketEndpoint('wss://wbs.mexc.com/ws'),
    ]
    rest_endpoints = [RestEndpoint('https://api.mexc.com', routes=Routes('/api/v3/exchangeInfo'))]
    # valid_candle_intervals = {'1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '1d', '1w', '1M'}
    # candle_interval_map = {'1m': '1', '3m': '3', '5m': '5', '15m': '15', '30m': '30', '1h': '60', '2h': '120', '4h': '240', '6h': '360', '1d': 'D', '1w': 'W', '1M': 'M'}

    @classmethod
    def timestamp_normalize(cls, ts: Union[int, dt]) -> float:
        if isinstance(ts, int):
            return ts / 1000.0
        else:
            return ts.timestamp()

    @classmethod
    def _parse_symbol_data(cls, data: dict) -> Tuple[Dict, Dict]:
        ret = {}
        info = defaultdict(dict)
        for entry in data['symbols']:
            if not entry['status']:
                continue
            symbol = entry['symbol']
            base = entry['baseAsset']
            quote = entry['quoteAsset']
            s = Symbol(base=base, quote=quote, type=SPOT)
            ret[s.normalized] = symbol
        return ret, info

    def __reset(self, conn: AsyncConnection):
        if self.std_channel_to_exchange(L2_BOOK) in conn.subscription:
            for pair in conn.subscription[self.std_channel_to_exchange(L2_BOOK)]:
                std_pair = self.exchange_symbol_to_std_symbol(pair)

                if std_pair in self._l2_book:
                    del self._l2_book[std_pair]

        self._instrument_info_cache = {}

    async def message_handler(self, msg: str, conn, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)
        if msg.get('c', '').startswith('spot@public.limit.depth'):
            await self._book(msg, timestamp)
        else:
            LOG.warning("%s: Unhandled message type %s", conn.uuid, msg)

    async def subscribe(self, connection: AsyncConnection):
        self.__reset(connection)
        for chan in connection.subscription:
            if not self.is_authenticated_channel(self.exchange_channel_to_std(chan)):
                for pair in connection.subscription[chan]:
                    params = [chan.format(pair)]
                    await connection.write(json.dumps({"method": "SUBSCRIPTION", "params": params}))
            else:
                raise NotImplementedError

    async def _book(self, msg: dict, timestamp: float):
        pair = self.exchange_symbol_to_std_symbol(msg['s'].split('.')[-1])
        self._l2_book[pair] = OrderBook(self.id, pair, max_depth=self.max_depth)
        d = msg['d']
        for book in ['bids', 'asks']:
            for pv in d[book]:
                side = BID if book == 'bids' else ASK
                self._l2_book[pair].book[side][Decimal(pv['p'])] = Decimal(pv['v'])
        ts = msg['t']
        if isinstance(ts, str):
            ts = int(ts)
        await self.book_callback(L2_BOOK, self._l2_book[pair], timestamp, timestamp=ts / 1000, raw=msg)

    async def _order(self, msg: dict, timestamp: float):
        raise NotImplementedError
        # order_status = {
        #     'Created': SUBMITTING,
        #     'Rejected': FAILED,
        #     'New': OPEN,
        #     'PartiallyFilled': PARTIAL,
        #     'Filled': FILLED,
        #     'Cancelled': CANCELLED,
        #     'PendingCancel': CANCELLING
        # }

        # for i in range(len(msg['data'])):
        #     data = msg['data'][i]

        #     oi = OrderInfo(
        #         self.id,
        #         self.exchange_symbol_to_std_symbol(data['symbol']),
        #         data["order_id"],
        #         BUY if data["side"] == 'Buy' else SELL,
        #         order_status[data["order_status"]],
        #         LIMIT if data['order_type'] == 'Limit' else MARKET,
        #         Decimal(data['price']),
        #         Decimal(data['qty']),
        #         Decimal(data['qty']) - Decimal(data['cum_exec_qty']),
        #         self.timestamp_normalize(data.get('update_time') or data.get('O') or data.get('timestamp')),
        #         raw=data,
        #     )
        #     await self.callback(ORDER_INFO, oi, timestamp)

    async def authenticate(self, conn: AsyncConnection):
        if any(self.is_authenticated_channel(self.exchange_channel_to_std(chan)) for chan in conn.subscription):
            auth = self._auth(self.key_id, self.key_secret)
            LOG.debug(f"{conn.uuid}: Sending authentication request with message {auth}")
            await conn.write(auth)

    def _auth(self, key_id: str, key_secret: str) -> str:

        expires = int((time.time() + 60)) * 1000
        signature = str(hmac.new(bytes(key_secret, 'utf-8'), bytes(f'GET/realtime{expires}', 'utf-8'), digestmod='sha256').hexdigest())
        return json.dumps({'op': 'auth', 'args': [key_id, expires, signature]})
