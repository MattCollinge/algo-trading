from chdb.session import Session

print("Starting chDB...")
# create a persistent session
db = Session(path="/tmp/oanda_data") #/tmp/quantDB")

# create a database and a table
db.query("create database IF NOT EXISTS quant")
db.query("""
create table IF NOT EXISTS quant.ohlc (
  instrument LowCardinality(String),
  tf LowCardinality(String),
  open Decimal(8,5),
  high Decimal(8,5),
  low Decimal(8,5),
  close Decimal(8,5),
  timestamp DateTime('EST'),
) engine MergeTree
PRIMARY KEY (instrument, tf, timestamp);
""")

# # load data into the table
# db.query("""
# insert into quant.ohlc
# select * from 'employees.csv'
# """)

# db.query("""INSERT INTO quant.ohlc (instrument, tf, open, high, low, close, timestamp) VALUES ('EURUSD', 'S5', 1.00001, 1.99999, 0.77777, 1.05432, Now()), ('EURUSD', 'S5', 1.05432, 1.99999, 0.77777, 1.05433, yesterday()), ('EURUSD', 'S5', 1.05433, 1.99999, 0.77777, 1.05432, today()); """)

# # ...
# # restore the session later
# db = Session(path="/tmp/quantDB")

# query the data
res = db.query("select count(*) from quant.ohlc")
# res = db.query("select * from quant.ohlc")
print(res, end="")

# res = db.query("TRUNCATE TABLE quant.ohlc")
# print(res, end="")

res = db.query("select TOP 180 * from quant.ohlc Order by timestamp desc")
print(res, end="")