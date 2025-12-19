import pandas as pd
from sqlalchemy import create_engine
import os
import sys

ENRICH_PATH = "/opt/airflow/data/tripdata_enriched.csv"

def main():
    # 1. Load enriched CSV
    if not os.path.exists(ENRICH_PATH):
        print(f"Enriched CSV not found at {ENRICH_PATH}. Ensure enrich task ran.")
        sys.exit(1)

    print("Loading enriched trip data CSV...")
    df = pd.read_csv(ENRICH_PATH)
    print(f"Records to load: {len(df)}")

    # 2. Connect to Postgres (Docker service)
    engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")
    print("Connected to PostgreSQL successfully.")

    # 3. Load into analytics-ready table
    df.to_sql(
        name="tripdata_enriched",
        con=engine,
        if_exists="replace",   # overwrite safely
        index=False
    )

    print(f"SUCCESS: tripdata_enriched table created with {len(df)} rows.")

if __name__ == "__main__":
    main()
