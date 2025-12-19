import os
from clickhouse_connect import get_client

def main():
    try:
        print("Connecting to ClickHouse...")
        client = get_client(
            host="github.demo.trial.altinity.cloud", 
            port=8443, 
            username="demo", 
            password="demo", 
            secure=True
        )

        query = """SELECT * 
FROM tripdata
WHERE pickup_date >= '2009-01-01'
  AND pickup_date < '2016-01-01'
ORDER BY RAND()
LIMIT 10000;"""

        df = client.query_df(query)

        output_path = "/opt/airflow/data/tripdata_raw.csv"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)

        print(f"Rows extracted: {len(df)}")
        print(df.head())

    except Exception as e:
        print(f"ETL Failed: {e}")
        raise

if __name__ == "__main__":
    main()
