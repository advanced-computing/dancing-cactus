from __future__ import annotations

import datetime

import pandas as pd
import streamlit as st

import pandas_gbq
from google.oauth2 import service_account

from bigquery_utils import load_henry_hub_from_bigquery

# ------ Page config ------
st.set_page_config(page_title="Energy Market Analysis", layout="wide")


# ------ Constants ------
NYISO_BASE_URL = (
    "https://mis.nyiso.com/public/csv/realtime/{month}01realtime_zone_csv.zip"
)

EIA_HENRY_HUB_BASE_URL = "https://api.eia.gov/v2/natural-gas/pri/fut/data/"

# ------ Credentials ------
creds = st.secrets["gcp_service_account"]
credentials = service_account.Credentials.from_service_account_info(creds)


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
def load_nyiso_realtime(selected_month) -> any:
    start_date = datetime.datetime.strptime(selected_month, "%Y-%m-%d")

    if start_date.month == 12:
        end_date = datetime.datetime(start_date.year + 1, 1, 1)
    else:
        end_date = datetime.datetime(start_date.year, start_date.month + 1, 1)

    sql = f"""
    SELECT Time_Stamp, Name, LBMP____MWHr_ 
    FROM `sipa-adv-c-dancing-cactus.dataset.market_analysis` 
    WHERE Time_Stamp >= '{start_date.strftime("%Y-%m-%d")}'
    AND Time_Stamp < '{end_date.strftime("%Y-%m-%d")}'
    """
    df = pandas_gbq.read_gbq(sql, credentials=credentials)

    return df


@st.cache_data(ttl=3600)
def load_henry_hub_data() -> pd.DataFrame:
    df = load_henry_hub_from_bigquery().copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["date", "price"]).sort_values("date")

    if "series_description" in df.columns:
        df = df[
            df["series_description"].str.contains(
                "Henry Hub Natural Gas Spot Price",
                case=False,
                na=False,
            )
        ].copy()

    return df


# ------ Metric functions -------
def compute_electricity_metrics(df: pd.DataFrame) -> dict[str, str]:
    avg_price = df["LBMP____MWHr_"].mean()
    max_price = df["LBMP____MWHr_"].max()
    min_price = df["LBMP____MWHr_"].min()

    peak_row = df.loc[df["LBMP____MWHr_"].idxmax()]
    peak_hour = peak_row["Time_Stamp"].strftime("%Y-%m-%d %H:%M")

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
def electricity_interpretation(df: pd.DataFrame, Name: str) -> str:
    peak_row = df.loc[df["LBMP____MWHr_"].idxmax()]
    low_row = df.loc[df["LBMP____MWHr_"].idxmin()]

    peak_time = peak_row["Time_Stamp"].strftime("%Y-%m-%d %H:%M")
    low_time = low_row["Time_Stamp"].strftime("%Y-%m-%d %H:%M")

    return (
        f"For the selected NYISO zone ({Name}), real-time electricity prices fluctuate substantially over the month. "
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
        - gas: Henry Hub data stored in BigQuery
        """
    )


def render_intro() -> None:
    st.title("🏬 NYC Electricity Prices and Natural Gas Context")
    st.write(
        """
        This page combines two related parts of our project: NYISO real-time electricity prices and
        Henry Hub natural gas prices stored in BigQuery. The goal is to present a more coherent energy-market
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

    zones = sorted(realtime_df["Name"].dropna().unique().tolist())
    default_zone = "N.Y.C." if "N.Y.C." in zones else zones[0]

    zone = st.selectbox("Select a NYISO zone", zones, index=zones.index(default_zone))
    zone_df = realtime_df.loc[realtime_df["Name"] == zone].copy()
    zone_df = zone_df.sort_values("Time_Stamp")

    if zone_df.empty:
        st.warning("No electricity data available for the selected zone.")
        return

    metrics = compute_electricity_metrics(zone_df)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Average LBMP", metrics["avg"])
    c2.metric("Maximum LBMP", metrics["max"])
    c3.metric("Minimum LBMP", metrics["min"])
    c4.metric("Peak Timestamp", metrics["peak_hour"])

    chart_df = zone_df.set_index("Time_Stamp")[["LBMP____MWHr_"]]
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
        This section uses Henry Hub daily prices loaded from BigQuery. Henry Hub is treated here as a benchmark
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
        "Henry Hub price is a benchmark U.S. natural gas series loaded from BigQuery."
    )

    st.write("**Interpretation**")
    st.write(gas_interpretation(filtered))

    with st.expander("Preview processed gas data"):
        st.dataframe(filtered.head(30), use_container_width=True)

    st.divider()


def render_gas_unavailable(exc: Exception) -> None:
    st.header("Natural Gas Benchmark Context")
    st.warning(
        "Henry Hub gas data could not be loaded from BigQuery in the current runtime environment. "
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
            benchmark fuel-market context, but it is unavailable in the current runtime because the BigQuery query
            failed or credentials were not configured correctly.
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
    try:
        nyiso_month_datetime = datetime.datetime.strptime(nyiso_month, "%Y%m")

        if nyiso_month_datetime < datetime.datetime(2017, 1, 1):
            st.error("No data available. Please fill months after 2017")
            st.stop()

        selected_month = nyiso_month_datetime.strftime("%Y-%m-%d")

    except ValueError:
        st.error("Invalid form. Please write in YYYYMM")
        st.stop()

    render_intro()

    try:
        realtime_df = load_nyiso_realtime(selected_month)
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
