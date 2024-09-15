import backtrader as bt
import backtrader.indicators as btind
import backtrader.feeds as btfeeds
import json
import datetime  # For datetime objects

# https://github.com/happydasch/btoandav20
import btoandav20 as bto
import pytz
import argparse
import storageStrategy 

def runBackTesterStrategy(instrument, fromdate, todate):
    # print(instrument, fromdate, todate)

    with open("../secret/config-live.json", "r") as file:
        config = json.load(file)

    storekwargs = dict(
        token=config["oanda"]["token"],
        account=config["oanda"]["account"],
        practice=config["oanda"]["practice"],
        notif_transactions=True,
        stream_timeout=20,
    )

    tzData = pytz.timezone('US/Eastern')#'Europe/London') #'US/Eastern') 
    # fromdate2 = tzData.localize(fromdate)
    # todate2 = tzData.localize(todate)

    datakwargs = dict(
        historical=True,
        fromdate=fromdate,
        todate=todate,
        timeframe=bt.TimeFrame.Seconds,
        compression=5,
        bidask=False, #Use Mid Price
        tz='US/Eastern', #'US/Eastern', 'Europe/Berlin'
    )

    store = bto.stores.OandaV20Store(**storekwargs)
    data = bto.feeds.OandaV20Data(dataname=instrument, **datakwargs)

    stratkwargs = dict(
        tzData=tzData, 
        instrument=instrument,
        timeframe="S5"
    )

    cerebro = bt.Cerebro()
    cerebro.adddata(data)
    cerebro.addstrategy(storageStrategy.storageStrategy, **stratkwargs)

    # Run over everything
    strats = cerebro.run()

def main():
    parser = argparse.ArgumentParser("oanda downloader")
    parser.add_argument("Instrument", help="The Instrument to download historical data for", type=str)
    parser.add_argument("FromYear", help="The Year part of the Date to request Instrument Data From", type=int)
    parser.add_argument("FromMonth", help="The Month part of the Date to request Instrument Data From", type=int)
    parser.add_argument("FromDay", help="The Day part of the Date to request Instrument Data From", type=int)
    parser.add_argument("ToYear", help="The Year part of the Date to request Instrument Data To", type=int)
    parser.add_argument("ToMonth", help="The Month part of the Date to request Instrument Data To", type=int)
    parser.add_argument("ToDay", help="The Day part of the Date to request Instrument Data To", type=int)


    args = parser.parse_args()

    instrument = args.Instrument #"EUR_USD"
    fromdate = datetime.datetime(args.FromYear, args.FromMonth, args.FromDay)
    todate = datetime.datetime(args.ToYear, args.ToMonth, args.ToDay)
    print(datetime.datetime.today().isoformat(" ","seconds"),":", "Executing  with:", instrument, fromdate, todate)


    runBackTesterStrategy(instrument, fromdate, todate)


if __name__ == "__main__":
    main()


# Example cmd Line: python oanda_mongo_downloader.py EUR_USD 2023 11 20 2023 11 21