import duckdb

conn = duckdb.connect(f"traffic_crash_db.duckdb")
conn.sql(f"SET search_path = 'raw'")
# print(conn.sql("select person_id, crash_date, count(*) from fatalities group by person_id, crash_date"))
# print(conn.sql("Select any_value(person_id) ,count(1) from peoples"))
# print(conn.sql("select * from fatalities "
#                "QUALIFY row_number() over (partition by person_id  order by person_id,crash_date) = 1"
#                ).to_csv(file_name="fatalities.csv", header=True, sep=",", ))
# print(conn.sql("select * from peoples").columns)
# print(conn.sql("select * from vehicles").columns)
# conn.sql
#
# print(conn.sql("select * from dim_fatalities "))
# .to_parquet(file_name="fatalities")
# print(conn.sql("select vehicle_id, count(*) from vehicles group by vehicle_id"))
# .to_parquet(file_name="vehicles")
# print(conn.sql("select * from dim_peoples "))
# print(conn.sql("""select count(*) from dim_crashes
# where crash_month=6 and
# crash_day_of_week=1 and
# crash_hour between ? and ?
#  limit 10"""))
# .to_parquet(file_name="crashes")-
import datetime

date = datetime.date.today().strftime("%Y%m%d")
# print(date)
# print(type(date))
import pandas as pd

df = pd.read_parquet("E:\\Ajay\\spark-data\\traffic_crash")

# "E:\Ajay\spark-data\traffic_crash"
print(conn.sql("""
select * from dim_vehicles
where month(crash_date) =6   and 

hour(crash_date) between 3 and 16"""
))
