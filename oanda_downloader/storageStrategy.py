import backtrader as bt
# import archive.mongo_atlas as mongo_atlas
import chDB_engine
import datetime 
from threading import Timer, Condition, RLock

class RepeatTimer(Timer):
    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)

class storageStrategy(bt.Strategy):

    OHLCRepo = None
    bars = None
    t = None
    zero_count = 0
    lock = None

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

        # dt = bt_bar.datetime.datetime(0).astimezone(timezone)
        dt = bt_bar.datetime.datetime(0) #.astimezone(timezone)
        dtString = dt

        bar = {"open": open,
               "high": high,
               "low": low,
               "close": close,
               "timestamp": dtString}
        
        # with self.condition:
        self.lock.acquire()
        self.bars.append(bar)
        self.lock.release()


    def flush(self):
        self.lock.acquire()

        if len(self.bars) == 0 and self.zero_count > 4:
            self.t.cancel()
            print(datetime.datetime.today().isoformat(" ","seconds"),":",'Batch completed, Timer Cancelled')
            return
        
        if len(self.bars) == 0:
            self.zero_count = self.zero_count + 1
            print(datetime.datetime.today().isoformat(" ","seconds"),":",'Timer fired:',  self.zero_count)
        else:
            self.zero_count = 0
            self.OHLCRepo.insertOHLCs(self.bars)
            self.bars.clear()
        
        self.lock.release()        

    def __init__(self):
        self.bars = []
        self.zero_count = 0
        self.lock = RLock()
        self.t = RepeatTimer(5, self.flush)
        self.t.start()
        # self.OHLCRepo = mongo_atlas.OHLCMongo(self.p.instrument, self.p.timeframe)
        self.OHLCRepo = chDB_engine.OHLC_CHDB(self.p.instrument, self.p.timeframe)
        self.OHLCRepo.connect()
        

    def next(self):
        self.log()