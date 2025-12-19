import pandas as pd
from sqlalchemy import create_engine

# How many passengers do we carry per day?

engine = create_engine(
    "postgresql://urbanmove:urbanmove123@localhost:5432/urbanmove_db"
)

query = """
SELECT
    pickup_date,
    COUNT(*) AS total_trips,
    SUM(passenger_count) AS total_passengers
FROM clean.tripdata
GROUP BY pickup_date
ORDER BY pickup_date;
"""

df = pd.read_sql(query, engine)

df.to_sql(
    name="daily_ridership",
    con=engine,
    if_exists="replace",
    index=False
)

print(" Analytics table created")
