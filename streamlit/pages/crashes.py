from timeit import default_timer as timer

import duckdb
import streamlit as st

# st.set_page_config(page_title="Crashes", page_icon="💥")

st.markdown("# Crashes Data")
st.sidebar.header("Crashes")
start_timer = timer()
conn = duckdb.connect(database="../traffic_crash_db.duckdb", read_only=True)

# st.set_page_config(layout='wide')


st.subheader("Filters")

col_mon, col_wk = st.columns(2)

with col_mon:
    month_df = conn.execute("""
    select distinct crash_month from dim_crashes order by crash_record_id """).df()
    Month = st.selectbox('Month: ', month_df)

with col_wk:
    week_df = conn.execute("""
    select distinct crash_day_of_week from dim_crashes order by crash_record_id """).df()
    Week = st.selectbox('Day of Week(sunday=1): ', week_df)

(slider_min, slider_max) = st.slider(
    "Crash Time Interval:",
    min_value=0,
    max_value=23,
    value=(0, 23),
    format="hh")

st.write("Start time:", slider_min, slider_max)

st.subheader("Data Preview: ")

main_table_count = conn.execute("""
select count(*) from dim_crashes
where crash_month=?  and
crash_day_of_week=?  and
crash_hour between ? and ?""", [Month, Week, slider_min, slider_max]).fetchone()[0]

main_table_head = conn.execute("""
select * from dim_crashes
where crash_month=? and 
crash_day_of_week=? and 
crash_hour between ? and ? """, [Month, Week, slider_min, slider_max]).df()
end_timer = timer()
st.write("Total Number of rows: ", main_table_count)
st.write("Crash-Data : ", main_table_head)

table_data = conn.execute("""
select * from dim_crashes
where crash_month=? and 
crash_day_of_week=? and 
crash_hour between ? and ? and latitude::numeric is not null and longitude::numeric is not null  """,
                          [Month, Week, slider_min, slider_max]).df()
new_Data = table_data[['latitude', 'longitude', 'num_units', 'crash_hour', 'street_no', 'beat_of_occurrence']].astype(
    'float')

st.write("Total running time: ", end_timer - start_timer, " seconds")

st.write("Map showing where crashes occurred: ")
new_Data.loc[:, 'latitude'] = table_data['latitude'].astype('float')
new_Data.loc[:, 'longitude'] = table_data['longitude'].astype('float')
new_Data.loc[:, 'num_units'] = table_data['num_units'].astype('float')
# new_Data.loc[:, 'crash_hour'] = table_data['crash_hour'].astype('float') / int(100)
# new_Data.loc[:, 'street_no'] = table_data['street_no'].astype('float') / int(10000)
# new_Data.loc[:, 'beat_of_occurrence'] = table_data['beat_of_occurrence'].astype('float') / int(10000)
# color_list = new_Data[['num_units', 'crash_hour', 'street_no', 'beat_of_occurrence']].values.tolist()


st.map(data=new_Data, latitude=table_data['latitude'], longitude=table_data['longitude'],
       use_container_width=True, size='num_units')
