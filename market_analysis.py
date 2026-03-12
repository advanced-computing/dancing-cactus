from __future__ import annotations

import os
from datetime import date
from io import BytesIO
from zipfile import ZipFile

import pandas as pd
import requests
import streamlit as st


# ------ Page config ------
st.set_page_config(page_title="Energy Market Analysis", layout="wide")


# ------ Constants ------
NYISO_BASE_URL = (
    "https://mis.nyiso.com/public/csv/realtime/{month}01realtime_zone_csv.zip"
)

EIA_HENRY_HUB_BASE_URL = "https://api.eia.gov/v2/natural-gas/pri/fut/data/"


# ------ Utility helpers ------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(col).strip() for col in df.columns]
    return df


def find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lower_map = {col.lower(): col for col in df.columns}
    for candidate in candidates:
        if candidate.lower() in lower_map:
            return lower_map[candidate.lower()]
    return None


def get_eia_api_key() -> str:
    """
    Try Streamlit secrets first, then environment variable.
    """
    # 1) Streamlit secrets
    try:
        if "EIA_API_KEY" in st.secrets:
            key = str(st.secrets["EIA_API_KEY"]).strip()
            if key:
                return key
    except Exception:
        pass

    # 2) Environment variable
    key = os.getenv("EIA_API_KEY", "").strip()
    if key:
        return key

    raise ValueError(
        "Missing EIA_API_KEY. Please set it in .streamlit/secrets.toml "
        "or export it in the same terminal session before running Streamlit."
    )


# ------ API loaders -------
@st.cache_data(ttl=3600)
def load_nyiso_realtime_month(month: str) -> pd.DataFrame:
    url = NYISO_BASE_URL.format(month=month)

    response = requests.get(url, timeout=60)
    response.raise_for_status()

    zip_bytes = BytesIO(response.content)

    frames = []
    with ZipFile(zip_bytes) as zf:
        csv_names = [name for name in zf.namelist() if name.endswith(".csv")]

        if not csv_names:
            raise ValueError("No CSV files found inside NYISO zip archive.")

        for csv_name in csv_names:
            with zf.open(csv_name) as f:
                df = pd.read_csv(f)
                frames.append(df)

    if not frames:
        raise ValueError("NYISO realtime zip did not contain readable data.")

    combined = pd.concat(frames, ignore_index=True)
    combined = normalize_columns(combined)

    time_col = find_column(combined, ["Time Stamp", "Timestamp", "time_stamp", "time"])
    zone_col = find_column(combined, ["Name", "Zone", "zone", "name"])
    price_col = find_column(combined, ["LBMP ($/MWHr)", "LBMP", "lbmp"])

    if time_col is None or zone_col is None or price_col is None:
        raise ValueError(
            "Could not identify NYISO columns. "
            "Expected columns similar to Time Stamp, Name, and LBMP ($/MWHr)."
        )

    combined = combined[[time_col, zone_col, price_col]].copy()
    combined.columns = ["timestamp", "zone", "lbmp"]

    combined["timestamp"] = pd.to_datetime(combined["timestamp"], errors="coerce")
    combined["lbmp"] = pd.to_numeric(combined["lbmp"], errors="coerce")
    combined = combined.dropna(subset=["timestamp", "zone", "lbmp"]).copy()

    return combined


@st.cache_data(ttl=3600)
def load_henry_hub_data(start_date: str = "1993-12-24") -> pd.DataFrame:
    """
    Load Henry Hub natural gas prices from EIA API.
    """
    api_key = get_eia_api_key()
    today_str = date.today().isoformat()

    params = {
        "api_key": api_key,
        "frequency": "daily",
        "data[0]": "value",
        "start": start_date,
        "end": today_str,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 5000,
    }

    response = requests.get(EIA_HENRY_HUB_BASE_URL, params=params, timeout=60)
    response.raise_for_status()

    payload = response.json()

    if "response" not in payload or "data" not in payload["response"]:
        raise ValueError(f"Unexpected EIA API response format: {payload}")

    records = payload["response"]["data"]
    if not records:
        raise ValueError("EIA API returned no Henry Hub records.")

    df = pd.DataFrame(records)
    df = normalize_columns(df)

    period_col = find_column(df, ["period"])
    value_col = find_column(df, ["value"])
    series_col = find_column(
        df, ["series-description", "seriesDescription", "series description"]
    )

    if period_col is None or value_col is None:
        raise ValueError(
            f"Could not identify 'period' and 'value' columns in EIA response. "
            f"Columns found: {list(df.columns)}"
        )

    keep_cols = [period_col, value_col]
    if series_col:
        keep_cols.append(series_col)

    df = df[keep_cols].copy()

    rename_map = {
        period_col: "date",
        value_col: "price",
    }
    if series_col:
        rename_map[series_col] = "series_description"

    df = df.rename(columns=rename_map)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["date", "price"]).copy()
    df = df.sort_values("date")

    return df


# ------ Metric functions -------
def compute_electricity_metrics(df: pd.DataFrame) -> dict[str, str]:
    avg_price = df["lbmp"].mean()
    max_price = df["lbmp"].max()
    min_price = df["lbmp"].min()

    peak_row = df.loc[df["lbmp"].idxmax()]
    peak_hour = peak_row["timestamp"].strftime("%Y-%m-%d %H:%M")

    return {
        "avg": f"{avg_price:.2f}",
        "max": f"{max_price:.2f}",
        "min": f"{min_price:.2f}",
        "peak_hour": peak_hour,
    }


def compute_gas_metrics(df: pd.DataFrame) -> dict[str, str]:
    avg_price = df["price"].mean()
    max_price = df["price"].max()
    min_price = df["price"].min()

    peak_row = df.loc[df["price"].idxmax()]
    peak_date = peak_row["date"].strftime("%Y-%m-%d")

    return {
        "avg": f"{avg_price:.2f}",
        "max": f"{max_price:.2f}",
        "min": f"{min_price:.2f}",
        "peak_date": peak_date,
    }


# ------ Interpretation text ------
def electricity_interpretation(df: pd.DataFrame, zone: str) -> str:
    peak_row = df.loc[df["lbmp"].idxmax()]
    low_row = df.loc[df["lbmp"].idxmin()]

    peak_time = peak_row["timestamp"].strftime("%Y-%m-%d %H:%M")
    low_time = low_row["timestamp"].strftime("%Y-%m-%d %H:%M")

    return (
        f"For the selected NYISO zone ({zone}), real-time electricity prices fluctuate substantially over the month. "
        f"The highest observed price occurs at {peak_time}, while the lowest occurs at {low_time}. "
        f"These movements suggest changing short-run market conditions, including demand pressure, supply availability, "
        f"and locational system constraints."
    )


def gas_interpretation(df: pd.DataFrame) -> str:
    peak_row = df.loc[df["price"].idxmax()]
    low_row = df.loc[df["price"].idxmin()]

    peak_date = peak_row["date"].strftime("%Y-%m-%d")
    low_date = low_row["date"].strftime("%Y-%m-%d")

    return (
        f"The Henry Hub benchmark series shows substantial volatility over time. "
        f"In this sample, the highest observed benchmark gas price occurs on {peak_date}, "
        f"while the lowest occurs on {low_date}. Henry Hub is used here as a national benchmark "
        f"to provide broader fuel-market context rather than a New York-specific local gas price."
    )


# ------ Render sections ------
def render_sidebar() -> None:
    st.sidebar.title("Energy Market Dashboard")
    st.sidebar.markdown(
        """
        **This page combines**
        - NYISO real-time electricity prices
        - EIA Henry Hub natural gas prices
        - exploratory market interpretation

        **Data source logic**
        - electricity: online NYISO public data
        - gas: online EIA API
        """
    )


def render_intro() -> None:
    st.title("🏬 NYC Electricity Prices and Natural Gas Context")
    st.write(
        """
        This page combines two related parts of our project: NYISO real-time electricity prices and
        Henry Hub natural gas prices from the EIA API. The goal is to present a more coherent energy-market
        view by showing both local electricity price outcomes and broader benchmark fuel-market conditions.
        """
    )
    st.divider()


def render_electricity_section(realtime_df: pd.DataFrame) -> None:
    st.header("Electricity Market Overview")

    st.write(
        """
        This section explores hourly NYISO real-time electricity prices. Select a zone to examine
        how locational marginal prices vary over the available monthly sample.
        """
    )

    zones = sorted(realtime_df["zone"].dropna().unique().tolist())
    default_zone = "N.Y.C." if "N.Y.C." in zones else zones[0]

    zone = st.selectbox("Select a NYISO zone", zones, index=zones.index(default_zone))
    zone_df = realtime_df.loc[realtime_df["zone"] == zone].copy()
    zone_df = zone_df.sort_values("timestamp")

    if zone_df.empty:
        st.warning("No electricity data available for the selected zone.")
        return

    metrics = compute_electricity_metrics(zone_df)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Average LBMP", metrics["avg"])
    c2.metric("Maximum LBMP", metrics["max"])
    c3.metric("Minimum LBMP", metrics["min"])
    c4.metric("Peak Timestamp", metrics["peak_hour"])

    chart_df = zone_df.set_index("timestamp")[["lbmp"]]
    st.line_chart(chart_df, use_container_width=True)

    st.caption("LBMP is measured in dollars per megawatt-hour.")

    st.write("**Interpretation**")
    st.write(electricity_interpretation(zone_df, zone))

    with st.expander("Preview processed electricity data"):
        st.dataframe(zone_df.head(30), use_container_width=True)

    st.divider()


def render_gas_section(gas_df: pd.DataFrame) -> None:
    st.header("Natural Gas Benchmark Context")

    st.write(
        """
        This section uses Henry Hub daily prices from the EIA API. Henry Hub is treated here as a benchmark
        U.S. natural gas series, which provides fuel-market context for interpreting electricity price movements.
        """
    )

    min_date = gas_df["date"].min().date()
    max_date = gas_df["date"].max().date()

    selected_range = st.date_input(
        "Select gas date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    if isinstance(selected_range, tuple) and len(selected_range) == 2:
        start_date, end_date = selected_range
    else:
        start_date, end_date = min_date, max_date

    filtered = gas_df[
        (gas_df["date"].dt.date >= start_date) & (gas_df["date"].dt.date <= end_date)
    ].copy()

    if filtered.empty:
        st.warning("No gas data available for the selected date range.")
        return

    metrics = compute_gas_metrics(filtered)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Average Gas Price", metrics["avg"])
    c2.metric("Maximum Gas Price", metrics["max"])
    c3.metric("Minimum Gas Price", metrics["min"])
    c4.metric("Peak Date", metrics["peak_date"])

    chart_df = filtered.set_index("date")[["price"]]
    st.line_chart(chart_df, use_container_width=True)

    st.caption(
        "Henry Hub price is a benchmark U.S. natural gas series from the EIA API."
    )

    st.write("**Interpretation**")
    st.write(gas_interpretation(filtered))

    with st.expander("Preview processed gas data"):
        st.dataframe(filtered.head(30), use_container_width=True)

    st.divider()


def render_gas_unavailable(exc: Exception) -> None:
    st.header("Natural Gas Benchmark Context")
    st.warning(
        "Henry Hub gas data could not be loaded in the current runtime environment. "
        "Electricity content is still available below the intro section."
    )
    st.code(str(exc))


def render_comparison_section(gas_available: bool) -> None:
    st.header("Why Compare These Two Series?")

    if gas_available:
        st.write(
            """
            NYISO real-time prices capture local power-market outcomes, while Henry Hub provides a benchmark
            fuel-market signal. Looking at both together helps us place electricity price volatility in a broader
            energy-market context. This comparison is exploratory and interpretive rather than a direct causal estimate.
            """
        )
        st.info(
            "Key idea: electricity prices show local short-run market outcomes, while Henry Hub shows broader benchmark fuel conditions."
        )
    else:
        st.write(
            """
            NYISO real-time prices capture local power-market outcomes. Henry Hub is intended to provide broader
            benchmark fuel-market context, but it is unavailable in the current runtime because the EIA API key
            was not detected or the API request failed.
            """
        )


# ------ Main ------
def main() -> None:
    render_sidebar()

    st.sidebar.subheader("Electricity Data Controls")
    nyiso_month = st.sidebar.text_input(
        "NYISO month (YYYYMM)",
        value="202602",
        help="Example: 202602 for February 2026",
    )

    render_intro()

    try:
        realtime_df = load_nyiso_realtime_month(nyiso_month)
    except Exception as exc:
        st.error(
            f"Failed to load NYISO electricity data from online public source: {exc}"
        )
        return

    # Electricity always renders if available
    render_electricity_section(realtime_df)

    gas_available = False
    try:
        gas_df = load_henry_hub_data()
        gas_available = True
        render_gas_section(gas_df)
    except Exception as exc:
        render_gas_unavailable(exc)

    render_comparison_section(gas_available=gas_available)


if __name__ == "__main__":
    main()
