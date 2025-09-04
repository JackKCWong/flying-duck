import pyarrow as pa
import pyarrow.flight as flight
import time
import random
from datetime import datetime
from pyarrow._flight import FlightUnavailableError

def connect_with_retry(max_attempts=5):
    for attempt in range(max_attempts):
        try:
            client = flight.connect("grpc://localhost:8815")
            return client
        except FlightUnavailableError:
            if attempt < max_attempts - 1:
                print(f"Connection attempt {attempt + 1} failed, retrying in 1 second...")
                time.sleep(1)
            else:
                raise

def execute_query(client, query):
    try:
        ticket = flight.Ticket(query.encode("utf-8"))
        reader = client.do_get(ticket)
        result = reader.read_all()
        return result
    except Exception as e:
        print(f"Query error: {str(e)}")
        return None

def wait_for_table(client, table_name, max_attempts=10):
    """Wait for table to be created before starting queries."""
    for attempt in range(max_attempts):
        result = execute_query(client, f"SELECT COUNT(*) FROM {table_name}")
        if result is not None:
            return True
        print(f"Waiting for table {table_name} to be created... (attempt {attempt + 1})")
        time.sleep(1)
    return False

def continuous_query(client):
    table_name = "concurrent_test"

    # Wait for table to be created
    if not wait_for_table(client, table_name):
        print("Table was not created in time. Exiting.")
        return

    queries = [
        f"SELECT COUNT(*) as total_rows FROM {table_name}",
        f"SELECT category, AVG(value) as avg_value FROM {table_name} GROUP BY category",
    ]

    while True:
        try:
            # Pick a random query to execute
            query = random.choice(queries)
            print(f"\n{datetime.now().isoformat()}: Executing query: {query}")

            result = execute_query(client, query)
            if result is not None:
                print("Query result:")
                print(result)

            # Random sleep between 0.5-2 seconds
            sleep_time = random.uniform(0.5, 2)
            time.sleep(sleep_time)

        except Exception as e:
            print(f"Error in continuous query: {str(e)}")
            time.sleep(1)

if __name__ == "__main__":
    print("Starting continuous query client...")
    client = connect_with_retry()
    execute_query(client, "select * from ice")