import pandas as pd
import os
import sys

CLEAN_PATH = "/opt/airflow/data/tripdata_clean.csv"
WEATHER_PATH = "/opt/airflow/data/nyc_weather.csv"
ENRICH_PATH = "/opt/airflow/data/tripdata_enriched.csv"

def main():
    # 1. Load cleaned trip data CSV
    if not os.path.exists(CLEAN_PATH):
        print(f"Clean CSV not found at {CLEAN_PATH}. Ensure clean task ran.")
        sys.exit(1)

    print("Loading cleaned trip data...")
    trip_df = pd.read_csv(CLEAN_PATH)

    # 2. Load weather CSV
    if not os.path.exists(WEATHER_PATH):
        print(f"Weather CSV not found at {WEATHER_PATH}.")
        sys.exit(1)

    print(f"Loading weather data from {WEATHER_PATH}...")
    weather_df = pd.read_csv(WEATHER_PATH)

    # Standardize weather columns & parse dates
    weather_df.columns = weather_df.columns.str.strip().str.upper()
    weather_df['DATE'] = pd.to_datetime(weather_df['DATE']).dt.date
    weather_df = weather_df[['DATE', 'TMAX', 'TMIN', 'PRCP']]

    # 3. Create date column in trips to match weather
    trip_df['pickup_date'] = pd.to_datetime(trip_df['pickup_datetime']).dt.date

    # 4. Merge trips with weather
    print("Merging trips with weather conditions...")
    enriched_df = trip_df.merge(
        weather_df,
        how='left',
        left_on='pickup_date',
        right_on='DATE'
    )

    # 5. Rename columns & drop redundant DATE column
    enriched_df.rename(columns={
        'TMAX': 'temp_max',
        'TMIN': 'temp_min',
        'PRCP': 'precipitation'
    }, inplace=True)

    if 'DATE' in enriched_df.columns:
        enriched_df.drop(columns=['DATE'], inplace=True)

    # 6. Write enriched CSV
    os.makedirs(os.path.dirname(ENRICH_PATH), exist_ok=True)
    enriched_df.to_csv(ENRICH_PATH, index=False)

    print(f"SUCCESS: Enriched CSV written to {ENRICH_PATH}")
    print(f"Final enriched record count: {len(enriched_df)}")

if __name__ == "__main__":
    main()
