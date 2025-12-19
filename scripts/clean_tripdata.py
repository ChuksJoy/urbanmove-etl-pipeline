import pandas as pd
import os
import sys

RAW_PATH = "/opt/airflow/data/tripdata_raw.csv"
CLEAN_PATH = "/opt/airflow/data/tripdata_clean.csv"

def main():
    # 1. Load raw extracted CSV
    if not os.path.exists(RAW_PATH):
        print(f"Raw file not found at {RAW_PATH}. Ensure extract task ran.")
        sys.exit(1)

    print("Loading raw tripdata CSV...")
    df = pd.read_csv(RAW_PATH)
    print(f"Initial raw record count: {len(df)}")

    # 2. Column standardization
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # 3. Handle NULL values in mission-critical columns
    required_columns = ["pickup_datetime", "dropoff_datetime", "pickup_latitude", "pickup_longitude"]
    valid_cols = [c for c in required_columns if c in df.columns]
    df = df.dropna(subset=valid_cols)
    print(f"After dropping NULLs in critical columns: {len(df)}")

    # 4. Remove duplicates
    df = df.drop_duplicates()
    print(f"After removing duplicates: {len(df)}")

    # 5. GPS validation (coordinate scrubbing)
    gps_cols = ["pickup_latitude", "pickup_longitude"]
    if all(col in df.columns for col in gps_cols):
        df = df[
            (df["pickup_latitude"].between(-90, 90)) &
            (df["pickup_longitude"].between(-180, 180)) &
            (df["pickup_latitude"] != 0) &
            (df["pickup_longitude"] != 0)
        ]
        print(f"After filtering invalid GPS coordinates: {len(df)}")

    # 6. Timestamp correction & validation
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")
    df = df.dropna(subset=["pickup_datetime", "dropoff_datetime"])
    df = df[df["dropoff_datetime"] > df["pickup_datetime"]]
    print(f"After enforcing logical time order: {len(df)}")

    # 7. Trip duration validation (1 min ≤ duration ≤ 24 hrs)
    df["trip_duration_minutes"] = (df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds() / 60
    df = df[(df["trip_duration_minutes"] >= 1) & (df["trip_duration_minutes"] <= 1440)]
    print(f"After trip duration validation: {len(df)}")

    # 8. Numeric type coercion
    numeric_cols = ["pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude", "passenger_count", "fare_amount"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=[c for c in numeric_cols if c in df.columns])
    print(f"After numeric type enforcement: {len(df)}")

    # 9. Passenger count validation (1–6 passengers)
    if "passenger_count" in df.columns:
        df = df[df["passenger_count"].between(1, 6)]
    print(f"After passenger count validation: {len(df)}")

    # Drop unnecessary / junk columns
    drop_cols = [
        "junk1", 
        "junk2", 
        "store_and_fwd_flag", 
        "pickup_location_id", 
        "dropoff_location_id"
    ]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])
    print(f"Dropped unnecessary columns: {drop_cols}")

    # 10. Write cleaned CSV
    os.makedirs(os.path.dirname(CLEAN_PATH), exist_ok=True)
    df.to_csv(CLEAN_PATH, index=False)
    print(f"SUCCESS: Cleaned data written to {CLEAN_PATH}")
    print(f"Final cleaned record count: {len(df)}")

if __name__ == "__main__":
    main()
