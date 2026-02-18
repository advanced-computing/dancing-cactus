import streamlit as st
import pandas as pd

st.markdown("# Natural Gas Prices ❄️")
st.sidebar.markdown("# Natural Gas Prices ❄️")

st.write("Henry Hub Natural Gas Spot Price (USD per Million Btu)")

# Read Excel
gas = pd.read_excel(
    "data/RNGWHHDd.xls",
    sheet_name="Data 1",
    skiprows=3,  # Skip the first three row of Explanation
)

# Only keep first two col
gas = gas.iloc[:, 0:2]

# rename col
gas.columns = ["Date", "Price"]

gas = gas.dropna()

# change date format
gas["Date"] = pd.to_datetime(gas["Date"])

gas = gas.sort_values("Date")

st.line_chart(gas, x="Date", y="Price")


st.markdown("""
Natural gas prices are a key driver of electricity wholesale prices in NYC,
since gas-fired plants often set the marginal price in the power market.
""")
