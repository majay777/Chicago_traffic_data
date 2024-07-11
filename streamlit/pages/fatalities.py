from timeit import default_timer as timer

import duckdb
import streamlit as st
from streamlit_dynamic_filters import DynamicFilters

start_timer = timer()
st.markdown("# Fatalities From Crashes")
st.sidebar.header("Fatalities Filters: ")
conn = duckdb.connect(database="../traffic_crash_db.duckdb", read_only=True)

df = conn.execute("""
select *, month(crash_date) as Month, hour(crash_date) as Hour, year(crash_date) as Year from dim_fatalities""").df()
df['latitude'] = df['latitude'].astype('float64')
df['longitude'] = df['longitude'].astype('float64')
dynamic_filters = DynamicFilters(df, filters=['victim', 'crash_location', 'Month', 'Hour', 'Year'])
with st.sidebar:
    dynamic_filters.display_filters()
st.write("Total Number of rows: ", dynamic_filters.filter_df().count())
dynamic_filters.display_df()
st.write("Map showing where crashes occurred: ")

st.map(data=dynamic_filters.filter_df())
