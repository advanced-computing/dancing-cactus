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


# generate monthly URL
def get_zip(target_month):
    month = target_month.strftime("%Y%m01")
    zip_url = f"https://mis.nyiso.com/public/csv/realtime/{month}realtime_zone_csv.zip"
    return zip_url


# generate the name of target csv file
def get_csv(target_date):
    date_str = target_date.strftime("%Y%m%d")
    csv = f"{date_str}realtime_zone.csv"
    return csv


response = requests.get(get_zip(selected_date))

with zip.ZipFile(io.BytesIO(response.content), "r") as z:
    with z.open(get_csv(selected_date)) as f:
        data = pd.read_csv(f)


# pick up one area and clean the data
def process_nyiso_data(data, area):
    filetered_data = data[data["Name"] == area]
    filetered_data["Time Stamp"] = pd.to_datetime(
        filetered_data["Time Stamp"], format="%m/%d/%Y %H:%M:%S"
    )
    return filetered_data


# We focus on NYC for now.
specific_data = process_nyiso_data(data, "N.Y.C.")

hourly_data = (
    specific_data.resample("H", on="Time Stamp")["LBMP ($/MWHr)"]
    .mean()
    .reset_index(name="LBMP")
)
st.line_chart(
    data=hourly_data, x="Time Stamp", y="LBMP", x_label="Time", y_label="Price"
)
