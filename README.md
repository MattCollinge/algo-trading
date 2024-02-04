# algo-trading

`pip install backtrader`
`pip install git+https://github.com/happydasch/btoandav20`

Create a /secret folder and put in a config-practice.json file with the following filled in:

```{
    "oanda": {
        "token": "",
        "account": "",
        "practice": true
    }
}```


### Pipeline ###
- Call OandaAPI for Instrument for Time Period Stream S5 OHLC Data
- Store Raw S5 OHLC in chDB: Stream OHLC Data -> quant.ohl /tmp/oanda_data
- Load Raw S5 Data from chDB and Store in New Table: quant.ohlc /tmp/oanda_data -> quant.ohlc_opt /tmp/oanda_data
- Convert to Parquet: quant.ohlc_opt /tmp/oanda_data -> ./oanda_data_opt.parquet
- Load Parquet and Convert to DF: ./oanda_data_opt.parquet -> df
- Optimise for Panda types and Persist df as Parquet: df -> ./oanda_data_pandas.parquet
- Load Parquet into df for Enrichment: ./oanda_data_pandas.parquet -> df
- Loop
- Window over S5 Data and Aggregate to Target Interval in Daily Chunks with additonal fields: df -> chDB -> dfRes -> df
- Write the Chunk to a Parquet file: chDB dfRes -> oanda_data_intermediate.parquet 
- Insert the Parquet File into chDB Table: oanda_data_intermediate.parquet -> chDB quant.ohlc ./aggregated_oanda 
- Rinse Repeat
- Read in chDB Table as df and optimise datatypes for Pandas df: chDB quant.ohlc ./aggregated_oanda -> df
- Write out Final Parquet File: df -> oanda_{instrument}_{tf}.parquet
- do science





python run_generate_dailyOHLC.py
wsl
jupyter notebook --no-browser --NotebookApp.iopub_data_rate_limit 500000000.0

python oanda_downloader_runner.py EUR_USD 2017 1 1 2024 1 20

oanda_downloader.py EUR_USD 2017 1 1 2017 1 20









### Storage ###
http://chdb.dev/
https://arrow.apache.org/
https://antonz.org/trying-chdb/

```from pathlib import Path

query_sql = "select * from 'employees.csv'"
res = chdb.query(query_sql, "Parquet")

# export to Parquet
path = Path("/tmp/employees.parquet")
path.write_bytes(res.bytes())

# import from Parquet
query_sql = "select * from '/tmp/employees.parquet' limit 5"
res = chdb.query(query_sql, "PrettyCompactNoEscapes")
print(res, end="")```

Convert to pyArrow:
```query_sql = "select * from 'employees.csv'"
res = chdb.query(query_sql, "Arrow")

table = chdb.to_arrowTable(res)
print(table.schema)```

To persist a chDB session to a specific folder on disk, use the path constructor parameter. This way you can restore the session later:

```from chdb.session import Session

# create a persistent session
db = Session(path="/tmp/employees")

# create a database and a table
db.query("create database db")
db.query("""
create table db.employees (
  emp_id UInt32 primary key,
  first_name String, last_name String,
  birth_dt Date, hire_dt Date,
  dep_id String, city String,
  salary UInt32,
) engine MergeTree;
""")

# load data into the table
db.query("""
insert into db.employees
select * from 'employees.csv'
""")

# ...
# restore the session later
db = Session(path="/tmp/employees")

# query the data
res = db.query("select count(*) from db.employees")
print(res, end="")```

https://www.dataquest.io/blog/unit-tests-python/

Idea is:
1) Get Data from Oanda API via Backtrader at 5s resolution
2) Store it in Arraow Format using chDB locally
3) Upscale it to differnt HTF Bar sizes with info on when the H/L occured at lower resultion
4) Perform Staticially analysis and ML on the result sto see if there is an edge that can be leveraged.