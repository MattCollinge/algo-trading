from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

# For datetime objects
import datetime
import pytz


# Import the backtrader platform
import backtrader as bt
import btoandav20 as bto


class strategy(bt.Strategy):

    '''Initialization '''
    def __init__(self):
        self.live = True
        self.E1 = None
        self.SL1 = None
        self.TP1 = None

    def log(self, txt, dt=None):
        dt = dt or self.data.datetime.datetime(0)
        print('%s, %s' % (dt.isoformat(), txt))

    ''' Store notification '''
    def notify_store(self, msg, *args, **kwargs):
        print('-' * 50, 'STORE BEGIN')
        print(msg)
        print('-' * 50, 'STORE END')

    ''' Order notification '''
    def notify_order(self, order):
        if order.status in [order.Completed, order.Cancelled, order.Rejected]:
            print("ORDER IS DONE")
            self.order = None
        print('-' * 50, 'ORDER BEGIN')
        print(order)
        print('-' * 50, 'ORDER END')

    ''' Trade notification '''
    def notify_trade(self, trade):
        print('-' * 50, 'TRADE BEGIN')
        print(trade)
        print('-' * 50, 'TRADE END')

        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
            (trade.pnl, trade.pnlcomm))

    ''' Data notification '''
    def notify_data(self, data, status, *args, **kwargs):
        print('*' * 5, 'DATA NOTIF:', data._getstatusname(status), *args)
        if status == data.LIVE:
            self.live = True
        elif status == data.DELAYED:
            self.live = False

    def next(self):

        self.log('Next {}, {} {} {} {}'.format(len(self.data), self.data.open[0], self.data.high[0], self.data.low[0], self.data.close[0]))
        if self.live and self.E1 is None:
            #self.sizer = bto.sizers.OandaV20Risk(risk_percents=2, stoploss=10)
            #print(self.sizer.getsizing(data=self.data0, isbuy=True))
            #print(self.broker)
            self.E1 = self.buy(price=self.data.close[0] + 0.0005, size=100, exectype=bt.Order.Stop, transmit=False)
            #self.SL1 = self.sell(price=self.data.close[0] - 0.0005, size=100, exectype=bt.Order.Stop, transmit=False, parent=self.E1)
            self.TP1 = self.sell(price=self.data.close[0] + 0.0010, size=100, exectype=bt.Order.Limit, transmit=True, parent=self.E1)
            pass
        elif self.live:
            #self.close()
            pass

# Create a cerebro entity
cerebro = bt.Cerebro(quicknotify=True)

# Prepare oanda STORE
storekwargs = dict(
    token="",
    account="",
    practice=True,
)
store = bto.stores.OandaV20Store(**storekwargs)

# Prepare oanda data
datakwargs = dict(
    timeframe=bt.TimeFrame.Seconds,
    compression=5,
    tz=pytz.timezone('Europe/Berlin'),
    backfill=True,
    backfill_start=True,
)
data = store.getdata(dataname="EUR_USD", **datakwargs)
#data.resample(timeframe=bt.TimeFrame.Seconds, compression=30,rightedge=True,boundoff=1)
cerebro.adddata(data)

# Add broker
cerebro.setbroker(store.getbroker())

# Add strategy
cerebro.addstrategy(strategy)

cerebro.run()
