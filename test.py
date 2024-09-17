import duckdb

conn = duckdb.connect("traffic_crash_db.duckdb")
conn.sql(f"SET search_path = 'raw'")
# print(conn.sql("show tables"))
print(conn.sql("select * from  src_crashes order by crash_date desc"))
# conn.execute("copy src_vehicles FROM 'vehicles_full.csv' ")
# conn.execute("copy src_peoples FROM 'peoples_full.csv\\part-00000-f4af2bdb-f490-4196-8277-e2060c66a6d4-c000.csv' ")
# print(conn.sql("select count(*) from vehicles"))
# print(conn.sql("select count(*) from peoples"))
# print(conn.sql("select count(*) from crashes"))
# print(conn.sql("select * from peoples ").columns)
# print("src tables like vehicles, peoples, crashes")
# print(conn.sql("select count(*) from src_vehicles"))

# print(conn.sql("select count(*) from src_peoples"))
# print(conn.sql("select count(*) from src_crashes"))


# print("Dimensional tables like vehicles, peoples, crashes")
# print(conn.sql("select count(*) from dim_vehicles"))
# print(conn.sql("select count(*) from dim_peoples"))
# print(conn.sql("select count(*) from dim_crashes"))
# print(conn.sql("select * from crashes limit 10").columns)
# print(conn.sql("select * from vehicles limit 10").columns)
# print(conn.sql("select * from peoples limit 10").columns)
# print(conn.sql("select count(*) from src_crashes"))

# print(conn.execute("COPY src_crashes FROM 'New_crashes.parquet' (FORMAT PARQUET);"))

# conn.sql(" copy (SELECT    crash_record_id, strptime(crash_date, '%c%z') as crash_date, posted_speed_limit, "
#          "traffic_control_device, device_condition, weather_condition, "
#          "lighting_condition, first_crash_type, trafficway_type, alignment, "
#          "roadway_surface_cond, road_defect, report_type, crash_type, damage, "
#          "date_police_notified, prim_contributory_cause, sec_contributory_cause, "
#          "street_no, street_direction, street_name, beat_of_occurrence, num_units, "
#          "most_severe_injury, injuries_total, injuries_fatal, injuries_incapacitating,"
#          "injuries_non_incapacitating, injuries_reported_not_evident, injuries_no_indication, "
#          "injuries_unknown, crash_hour, crash_day_of_week, crash_month, latitude :: Numeric(11,9) as latitude, "
#          "longitude ::Numeric(11,9) as longitude, "
#          "location,  hit_and_run_i, intersection_related_i,"
#          "photos_taken_i, statements_taken_i, crash_date_est_i, private_property_i, work_zone_i,"
#          "work_zone_type, workers_present_i, dooring_i"
#          "  FROM 'New/crashes.parquet/part*.parquet') to 'New_crashes.parquet';")

# print(conn.sql("select count(*) from src_crashes"))
# print(conn.sql("COPY src_crashes "
#                " TO 'crashes1.parquet' "
#                "(FORMAT 'parquet', CODEC 'snappy');"))
