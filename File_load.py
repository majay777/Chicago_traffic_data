import duckdb

conn = duckdb.connect(f"traffic_crash_db.duckdb")
conn.sql(f"SET search_path = 'raw'")
conn.execute("INSERT INTO crashes    "
             "(SELECT * FROM read_json_auto('E:\\data\\traffic_crashes.json'));")
