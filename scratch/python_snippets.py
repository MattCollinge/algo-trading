

# Working Aggregation:
dfsample = df.iloc[1:100]

query_sql = """
select sq.interval, sq.open, sq.high, sq.low, sq.close, sq.high_timestamp, sq.low_timestamp, count(*) as samples 
from
    (select 
    CAST(date_trunc('minute', s5.timestamp) AS DateTime) as interval,
    FIRST_VALUE(s5.open) OVER (PARTITION BY interval ORDER BY s5.timestamp asc) open,
    FIRST_VALUE(s5.high) OVER (PARTITION BY interval ORDER BY s5.high desc) high,
    FIRST_VALUE(s5.low)  OVER (PARTITION BY interval ORDER BY s5.low asc) low,
    FIRST_VALUE(s5.close) OVER (PARTITION BY interval ORDER BY s5.timestamp desc) close,
    FIRST_VALUE(timestamp) OVER (PARTITION BY interval ORDER BY s5.high desc) high_timestamp,
    FIRST_VALUE(timestamp) OVER (PARTITION BY interval ORDER BY s5.low asc) low_timestamp
    from __s5__ as s5
    order by interval) as sq
group by sq.interval, sq.open, sq.high , sq.low, sq.close, sq.high_timestamp, sq.low_timestamp
order by sq.interval desc
;
"""

res = cdf.query(sql=query_sql, s5=dfsample)
dfres = res.to_pandas()
dfres['interval'] = dfres['interval'] * 1e9
dfres['interval'] = dfres['interval'].astype(pd.DatetimeTZDtype(tz=ZoneInfo('EST')))
dfres['high_timestamp'] = dfres['high_timestamp'].astype(pd.DatetimeTZDtype(tz=ZoneInfo('EST')))
dfres['low_timestamp'] = dfres['low_timestamp'].astype(pd.DatetimeTZDtype(tz=ZoneInfo('EST')))
dfres.head(30)



#################################



from chdb.session import Session
import pyarrow.feather as feather

print("Starting chDB...")
# create a persistent session
db = Session(path="/tmp/oanda_data") #/tmp/quantDB")

res = db.query("select instrument, tf, max(open) as open, max(high) as high, max(low) as low, max(close) as close, timestamp from quant.ohlc group by instrument, tf, timestamp Order by timestamp desc", "Arrow")
table = chdb.to_arrowTable(res)

feather.write_feather(table, './oanda_data.arrow')
print(table.schema)




import time
db = Session()
start = time.time()
res = db.query("select * from './oanda_data.arrow' order by timestamp desc", "Arrow")

end = time.time()
print(end - start)
print(
    f"{res.rows_read()} rows | "
    f"{res.bytes_read()} bytes | "
    f"{res.elapsed()} seconds"
)



query_sql = "select * from './oanda_data.arrow' order by timestamp desc limit 5"
res = chdb.query(query_sql, "PrettyCompactNoEscapes")
print(res, end="")





# import from Arrow
import pandas as pd
datafile = './oanda_data.arrow'
data = f"file('{datafile}', Arrow)"

sql = f"""SELECT * FROM '{datafile}' order by timestamp desc LIMIT 10"""
res = chdb.query(sql, "Arrow")
table = chdb.to_arrowTable(res)
print(table.schema)
df = table.to_pandas(types_mapper=pd.ArrowDtype)
print(df)
df.describe(include='all')
df.dtypes
# ret['close'].plot()


sql = f"""SELECT * FROM '{datafile}' order by timestamp desc LIMIT 10"""
res = chdb.query(query_sql, "Arrow")

frame = chdb.to_df(res)
frame.info()



# Run chDB on HTTP (Parquet, CSV, JSON ...)
!wget 'https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet' -q -O hits_0.parquet
import chdb

data = "url('https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet')"
# data = "file('hits_0.parquet', Parquet)"
# data = "s3('xxx')"

sql = f"""SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID)
                        FROM {data} GROUP BY RegionID ORDER BY c DESC LIMIT 10"""
ret = chdb.query(sql, 'dataframe')
print(ret)
ret.plot()