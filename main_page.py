import streamlit as st
import pandas as pd
from datetime import date, timedelta
import requests
import zipfile as zip
import io

# Main page content
st.markdown("# Main page 🎈")
st.sidebar.markdown("# Main page 🎈")

st.markdown("### Team")
st.markdown("- Jocelyn Jiang\n- Eisuke Kobayashi")
st.sidebar.markdown("### Team")
st.sidebar.markdown("Jocelyn Jiang, Eisuke Kobayashi")


selected_date = st.date_input(
    label="select the date", max_value=date.today() - timedelta(days=1)
)

st.title(f"Daily Wholesale Electricity Prices in NYC:{selected_date}")

month = selected_date.strftime("%Y%m01")
date = selected_date.strftime("%Y%m%d")

# We use real-time market LBMP data
# find a proper zip file
url = f"https://mis.nyiso.com/public/csv/realtime/{month}realtime_zone_csv.zip"
response = requests.get(url)

with zip.ZipFile(io.BytesIO(response.content), "r") as z:
    with z.open(f"{date}realtime_zone.csv") as f:
        data = pd.read_csv(f)

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
