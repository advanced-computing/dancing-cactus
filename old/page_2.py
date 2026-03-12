import streamlit as st
import pandas as pd


def clean_gas_df(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Clean Henry Hub natural gas price data.

    Expected input: a DataFrame where the first two columns correspond to
    date and price (as in the EIA-style Excel sheet after skiprows).
    Output: DataFrame with columns ["Date", "Price"], no NA rows, Date parsed,
    sorted ascending by Date.
    """
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["Date", "Price"])

    df = raw.iloc[:, 0:2].copy()
    df.columns = ["Date", "Price"]
    df = df.dropna()

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])  # drop rows where date failed to parse
    df = df.sort_values("Date").reset_index(drop=True)

    return df


def load_gas_data(excel_path: str) -> pd.DataFrame:
    """
    Read the Excel file and return the cleaned DataFrame.
    """
    raw = pd.read_excel(
        excel_path,
        sheet_name="Data 1",
        skiprows=3,
    )
    return clean_gas_df(raw)


def render_page(excel_path: str = "data/RNGWHHDd.xls") -> None:
    st.markdown("# Natural Gas Prices ❄️")
    st.sidebar.markdown("# Natural Gas Prices ❄️")

    st.write("Henry Hub Natural Gas Spot Price (USD per Million Btu)")

    gas = load_gas_data(excel_path)

    st.line_chart(gas, x="Date", y="Price")

    st.markdown(
        """
Natural gas prices are a key driver of electricity wholesale prices in NYC,
since gas-fired plants often set the marginal price in the power market.
"""
    )


if __name__ == "__main__":
    render_page()
