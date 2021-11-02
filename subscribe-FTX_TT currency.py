import time

from cryptofeed import FeedHandler
from cryptofeed.log import get_logger
from cryptofeed.exchanges import FTX
from cryptofeed.backends.mongo import TradeMongo, TickerMongo
from cryptofeed.defines import TRADES, TICKER
from config import *


def main():
    LOG = get_logger('main', LOGGING["filename"], level=LOGGING["level"])
    db_uri = DB_URI['FTX']
    current_time = str(int(time.time()))
    LOG.info("Connecting to %s", db_uri)
    data = {
            TRADES: TradeMongo("ftx", key='currency_trades_' + current_time, uri=db_uri),
            TICKER: TickerMongo("ftx", key='currency_ticker_' + current_time, uri=db_uri)
            }

    f = FeedHandler()
    f.add_feed(
        FTX(symbols=['BTC-USDT', 'ETH-USDT'],
               channels=list(data.keys()),
               callbacks=data))
    f.run()


if __name__ == '__main__':
    main()
