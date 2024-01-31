import logging
import time
from json import dumps

from kafka import KafkaProducer
from kiteconnect import KiteConnect, KiteTicker
import requests

from pipelines.kite.config.keys import API_KEY, ACCESS_CODE

from constants.KEYS_KITE import TRADING_SYMBOL, INSTRUMENT_TOKEN, CLOSE_PRICE

# logging.basicConfig(level=logging.DEBUG)

kite = KiteConnect(api_key=API_KEY)

kite.set_access_token(ACCESS_CODE)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

holdings = kite.holdings()

arr_holding_close_prices = []
arr_instrument_tokens = []
arr_trading_symbols = []

for holding in holdings:
    dictionary = {
        TRADING_SYMBOL: holding[TRADING_SYMBOL],
        INSTRUMENT_TOKEN: holding[INSTRUMENT_TOKEN],
        CLOSE_PRICE: holding[CLOSE_PRICE]
    }
    arr_instrument_tokens.append(holding[INSTRUMENT_TOKEN])
    arr_trading_symbols.append(holding[TRADING_SYMBOL])
    arr_holding_close_prices.append(dictionary)

print(arr_holding_close_prices)
print(arr_instrument_tokens)
print(arr_trading_symbols)

kws = KiteTicker(API_KEY, ACCESS_CODE)


def on_ticks(ws, ticks):  # noqa
    # Callback to receive ticks.
    # logging.info("Ticks: {}".format(ticks))
    for tick in ticks:
        print('Tick: ', tick)
        producer.send('testtopic', tick)


def on_connect(ws, response):  # noqa
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
    ws.subscribe(arr_instrument_tokens)

    # Set RELIANCE to tick in `full` mode.
    ws.set_mode(ws.MODE_LTP, arr_instrument_tokens)


def on_order_update(ws, data):
    logging.debug("Order update : {}".format(data))


# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_order_update = on_order_update

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
kws.connect()