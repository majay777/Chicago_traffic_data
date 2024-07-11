import streamlit as st

st.title('Chicago Traffic Crash Data')

crashes = st.page_link(
    label="Crashes", icon="💥", page="pages/crashes.py"
)
peoples = st.page_link(page="pages/peoples.py", label="Peoples in Crash", icon="👨")
vehicles = st.page_link(
    page="pages/vehicles.py", label="Vehicles in Crash", icon="🚌"
)

fatalities = st.page_link(page="pages/fatalities.py", label="Fatalities", icon="⚰️")
