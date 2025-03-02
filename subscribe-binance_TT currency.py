import time

from cryptofeed import FeedHandler
from cryptofeed.log import get_logger
from cryptofeed.exchanges import Binance
from cryptofeed.backends.mongo import TradeMongo, TickerMongo
from cryptofeed.defines import TRADES, TICKER 
from config import *

def main():
    LOG = get_logger('main', LOGGING["filename"], level=LOGGING["level"])
    db_uri = DB_URI['binance']
    current_time = str(int(time.time()))
    LOG.info("Connecting to %s", db_uri)
    data = {
            TRADES: TradeMongo("binance", key='trades_' + current_time, uri=db_uri)
            }

    f = FeedHandler()
    f.add_feed(
        Binance(symbols=['BTC-USDT', 'ETH-USDT'],
               channels=list(data.keys()),
               callbacks=data))
    f.run()


if __name__ == '__main__':
    main()
