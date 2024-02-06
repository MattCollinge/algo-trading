from processOHLC import * 

start = datetime(2017, 1, 1)
# start = datetime(2023, 12, 10) #2023-11-23 2023-03-18
end = datetime(2024, 1, 14)
instrument = 'EUR_USD'

# runProcessRawPipeline()

# clearOHLCTable()
minutes = 15
GenerateMinuteOHLC(instrument, start, end, minutes)
minutes = 5
GenerateMinuteOHLC(instrument, start, end, minutes)
minutes = 1
GenerateMinuteOHLC(instrument, start, end, minutes)

GenerateHourlyOHLC(instrument, start, end)
GenerateDailyOHLC(instrument, start, end)
GenerateWeeklyOHLC(instrument, start, end)