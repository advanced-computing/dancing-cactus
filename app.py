import streamlit as st
import pandas as pd
from datetime import date, timedelta

ten_days_ago = date.today() - timedelta(days=10)

selected_date = st.date_input(
    label="select the date", max_value=date.today(), min_value=ten_days_ago
)

st.title(f"Daily Wholesale Electricity Prices in NYC:{selected_date}")

date = selected_date.strftime("%Y%m%d")

# We use real-time market LBMP data
data = pd.read_csv(f"https://mis.nyiso.com/public/csv/realtime/{date}realtime_zone.csv")

# We focus on NYC for now.
data = data[data["Name"] == "N.Y.C."]

# convert columns to timestamps
data["Time Stamp"] = pd.to_datetime(data["Time Stamp"], format="%m/%d/%Y %H:%M:%S")
hourly_data = (
    data.resample("H", on="Time Stamp")["LBMP ($/MWHr)"].mean().reset_index(name="LBMP")
)
st.line_chart(
    data=hourly_data, x="Time Stamp", y="LBMP", x_label="Time", y_label="Price"
)
