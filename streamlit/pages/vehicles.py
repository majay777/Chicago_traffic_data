from timeit import default_timer as timer

import duckdb
import streamlit as st

# st.set_page_config(page_title="Crashes", page_icon="💥")

st.markdown("# Vehicles Data")
st.sidebar.header("Vehicles")

start_timer = timer()
conn = duckdb.connect(database="../traffic_crash_db.duckdb", read_only=True)

# st.set_page_config(layout='wide')


st.subheader("Filters")

col_mon, col_wk = st.columns(2)
# col_make

with col_mon:
    month_df = conn.execute("""
    select distinct month(crash_date) as crash_month from dim_vehicles order by crash_record_id, crash_unit_id """).df()
    Month = st.selectbox('Month: ', month_df)

with col_wk:
    week_df = conn.execute("""
    select distinct dayofweek(crash_date) as day_of_week from dim_vehicles order by crash_record_id,
     crash_unit_id """).df()
    Week = st.selectbox('Day of Week(sunday=0): ', week_df)

# with col_make:
#     make_df = conn.execute("""
#     select distinct make from dim_vehicles order by crash_record_id, crash_unit_id """).df()
#     Make = st.multiselect("Make Manufacture: ", make_df.values)

(slider_min, slider_max) = st.slider(
    "Crash Time Interval:",
    min_value=0,
    max_value=23,
    value=(0, 23),
    format="hh")

st.write("Start time:", slider_min, slider_max)

st.subheader("Data Preview: ")

main_table_count = conn.execute("""
select count(*) from src_vehicles
where month(crash_date) =?  and 
(dayofweek(crash_date))=?  and
hour(crash_date) between ? and ? """, [Month, Week, slider_min, slider_max]).fetchone()[0]

main_table_head = conn.execute("""
select * from src_vehicles
where month(crash_date) =?  and 
(dayofweek(crash_date))=?  and
hour(crash_date) between ? and ? order by crash_record_id, crash_unit_id""",
                               [Month, Week, slider_min, slider_max]).df()
end_timer = timer()
st.write("Total Number of rows: ", main_table_count)
st.write("Data : ", main_table_head)

st.write("Total running time: ", end_timer - start_timer, " seconds")
