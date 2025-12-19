from clickhouse_connect import get_client
from sqlalchemy import create_engine
import pandas as pd
# cleaning the datatype error i got trying to load to postgress

ch_client = get_client(
    host="github.demo.trial.altinity.cloud",
    port=8443,
    username="demo",
    password="demo",
    secure=True
)

# Extract data
# query = "SELECT * FROM tripdata LIMIT 1000"
# trip_df = ch_client.query_df(query)

# print(trip_df.dtypes)


#Verify random spool:

# df = pd.read_csv("data/tripdata_random.csv")
# print(df['pickup_datetime'].min(), df['pickup_datetime'].max())

# SELECT COUNT(*), MIN(pickup_datetime), MAX(pickup_datetime)
# FROM tripdata_clean;


# #sanity checks

# #Check row counts:

# SELECT COUNT(*) FROM tripdata_enriched;


# #check for missing weather values:

# SELECT COUNT(*) FROM tripdata_enriched
# WHERE temp_max IS NULL OR temp_min IS NULL;


# #Check distribution over years:

# SELECT EXTRACT(YEAR FROM pickup_datetime) AS year, COUNT(*)
# FROM tripdata_enriched
# GROUP BY year
# ORDER BY year;

# Check distribution over years:ClickHouse database
# query = """
# SELECT
#     toYear(pickup_datetime) AS year,
#     COUNT(*) AS cnt
# FROM tripdata
# GROUP BY year
# ORDER BY year
# """

# result = ch_client.query(query)
# for row in result.result_rows:
#     print(row)

