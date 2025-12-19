from sqlalchemy import create_engine
import pandas as pd

# Replace these with your PostgreSQL info
username = 'postgres'
password = 'pass123'
host = 'localhost'
port = 5432
database = 'urbanmove_db'

# Create connection engine
engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')

# Test connection by reading table names
with engine.connect() as conn:
    result = conn.execute("SELECT tablename FROM pg_tables WHERE schemaname='public';")
    tables = result.fetchall()
    print("Tables in PostgreSQL:", tables)
