import os

import adbc_driver_flightsql.dbapi

uri = "grpc://localhost:8815"
conn = adbc_driver_flightsql.dbapi.connect(uri)

with conn.cursor() as cur:
    cur.execute("SELECT 1")

    assert cur.fetchone() == (1,)

conn.close()
