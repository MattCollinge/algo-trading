import datetime
import sys, os
sys.path.insert(0, '../libs')

import pandas as pd
import backtrader as bt
import btoandav20


class TestX(bt.Strategy):
    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.dataclose = self.datas[0].close

    def notify_data(self, data, status, *args, **kwargs):
        print(" STATUS ************ DATA NOTIF: "+data._name+" "+data._getstatusname(status))

    def notify_store(self, msg, *args, **kwargs):
        print(" STORE NOTIF ------------- : "+str(msg))

    def next(self):
        print(self.data._name,self.data.datetime.datetime(),self.data.close[0])
        if self.dataclose[0] < self.dataclose[-1]:
            # current close less than previous close

            if self.dataclose[-1] < self.dataclose[-2]:
                # previous close less than the previous close

                # BUY, BUY, BUY!!! (with all possible default parameters)
                self.log('BUY CREATE, %.2f' % self.dataclose[0])
                self.buy()

def runstrat(args=None):
    pair = "EUR_USD"
    cerebro = bt.Cerebro()
    btoandav20.stores.OandaV20Store(
        token="1bd98774d6b1f5d62c249ff9743458a5-24ed7f4e4c0fd10581e1a84785b665fc",
        account="101-004-25763012-001", # EUR
        practice=True)
    data = btoandav20.feeds.OandaV20Data(dataname=pair,backfill=True,timeframe=bt.TimeFrame.Minutes, compression=1, fromdate=datetime.datetime(2023, 5, 1), todate=datetime.datetime(2023, 5, 16),)
    cerebro.broker = bt.brokers.BackBroker() #**eval('dict(' + args.broker + ')'))
    cerebro.adddata(data)
    cerebro.addstrategy(TestX)
    cerebro.run()
    cerebro.plot()

runstrat()
