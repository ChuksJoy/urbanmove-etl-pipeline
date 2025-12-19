# Step 1: Import the library
import clickhouse_connect
from clickhouse_connect import get_client

# Step 2: Connect to ClickHouse
client = get_client(
    host='github.demo.trial.altinity.cloud',  
    port=8443,                                       
    username='demo',                       
    password='demo',                  
    secure=True                                      
)

print("Connection Successful!")

# Step 3: Test the connection by listing tables
tables = client.query('SHOW TABLES').result_rows
print("Available tables in ClickHouse:", tables)

# Step 4: Perform a simple query
# Fetch demo tripdata into Pandas
query = 'SELECT * FROM tripdata LIMIT 5'
tripdata_df = client.query_df(query)

print("Tripdata sample:")
print(tripdata_df.head())