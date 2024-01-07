import backtrader as bt
import mongo
import datetime 
from threading import Timer

class RepeatTimer(Timer):
    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)

class mongoStrategy(bt.Strategy):

    OHLCRepo = None
    bars = None
    t = None

    params = dict(
        tzData=None,
        instrument="",
        timeframe=""
    )


    def log(self):
        ''' Logging function for this strategy'''
        timezone = self.p.tzData
        # print('log called')

        bt_bar = self.datas[0]

        open = self.datas[0].open[0]
        high = self.datas[0].high[0]
        low = self.datas[0].low[0]
        close = self.datas[0].close[0]

        # open = format(self.datas[0].open[0],'.%df' % self.datas[0].contractdetails['displayPrecision'])
        # high = format(self.datas[0].high[0],'.%df' % self.datas[0].contractdetails['displayPrecision'])
        # low = format(self.datas[0].low[0],'.%df' % self.datas[0].contractdetails['displayPrecision'])
        # close = format(self.datas[0].close[0],'.%df' % self.datas[0].contractdetails['displayPrecision'])
        # volume = self.datas[0].volume[0]

        dt = bt_bar.datetime.datetime(0).astimezone(timezone)
        dtString = dt

        bar = {"open": open,
               "high": high,
               "low": low,
               "close": close,
               "timestamp": dtString}
        
        self.bars.append(bar)
        # if len(self.bars)  == 200:
        #     self.flush()

    def flush(self):
        if len(self.bars) == 0:
            self.t.cancel()
            print(datetime.datetime.today().isoformat(" ","seconds"),":",'Batch completed, Timer Cancelled')
            return
        
        self.OHLCRepo.insertOHLCs(self.bars)
        # print('Mongo Inserted:',  len(self.bars), " Bars")
        self.bars.clear()
        

    def __init__(self):
        self.bars = []
        self.t = RepeatTimer(5, self.flush)
        self.t.start()
        self.OHLCRepo = mongo.OHLCMongo(self.p.instrument, self.p.timeframe)
        self.OHLCRepo.connect()
        

    def next(self):
        self.log()
        


   





 # truth will be called after a 15 second interval