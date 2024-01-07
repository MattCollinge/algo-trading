import subprocess
import datetime
import argparse
# oanda_mongo_downloader.py EUR_USD 2023 10 1 2023 10 10



def main():
    parser = argparse.ArgumentParser("oanda mongo downloader")
    parser.add_argument("Instrument", help="An integer will be increased by 1 and printed.", type=str)
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
    print(datetime.datetime.today().isoformat(" ","seconds"),":", "Executing oanda_mongo_downloader.py with:", instrument, fromdate, todate)

    batchDaySize = 10
    batchToDate = fromdate + datetime.timedelta(days=batchDaySize)
    if batchToDate > todate:
        batchToDate = todate

    batchFromDate = fromdate

    while batchToDate <= todate:
        result = subprocess.run(["python", "oanda_mongo_downloader.py", instrument, str(batchFromDate.year), str(batchFromDate.month), str(batchFromDate.day), str(batchToDate.year), 
                             str(batchToDate.month), str(batchToDate.day)], capture_output=True, text=True)
        print(result.stdout)
        print(result.stderr)
        batchFromDate = batchFromDate + datetime.timedelta(days=batchDaySize)
        batchToDate = batchToDate + datetime.timedelta(days=batchDaySize)
        if batchToDate > todate:
            batchToDate = todate + datetime.timedelta(days=1)

if __name__ == "__main__":
    main()