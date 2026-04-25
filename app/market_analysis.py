from __future__ import annotations

import datetime

import pandas as pd
import streamlit as st
import altair as alt

import pandas_gbq
from google.oauth2 import service_account

import json
import plotly.express as px

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from src.bigquery_utils import load_henry_hub_from_bigquery  # noqa: E402

# ------ Page config ------
st.set_page_config(page_title="Energy Market Analysis", layout="wide")


# ------ Constants ------
ZONE_MAP = {
    "WEST": "A - West (Buffalo/Niagara)",
    "GENESE": "B - Genesee (Rochester)",
    "CENTRL": "C - Central (Syracuse)",
    "NORTH": "D - North (St. Lawrence)",
    "MHK VL": "E - Mohawk Valley",
    "CAPITL": "F - Capital (Albany)",
    "HUD VL": "G - Hudson Valley",
    "MILLWD": "H - Millwood",
    "DUNWOD": "I - Dunwoodie",
    "N.Y.C.": "J - New York City",
    "LONGIL": "K - Long Island",
}

MAP_ZONE_MAP = {
    "WEST": "A",
    "GENESE": "B",
    "CENTRL": "C",
    "NORTH": "D",
    "MHK VL": "E",
    "CAPITL": "F",
    "HUD VL": "G",
    "MILLWD": "H",
    "DUNWOD": "I",
    "N.Y.C.": "J",
    "LONGIL": "K",
}

# ------ Credentials ------
creds = st.secrets["gcp_service_account"]
credentials = service_account.Credentials.from_service_account_info(creds)


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


@st.cache_data(ttl=3600)
def merge_load_and_lbmp(year: int, month: int) -> pd.DataFrame:
    start = datetime.datetime(year, month, 1)
    if month == 12:
        end = datetime.datetime(year + 1, 1, 1)
    else:
        end = datetime.datetime(year, month + 1, 1)

    sql_1 = f"""
    SELECT Time_Stamp, Name, Load 
    FROM `sipa-adv-c-dancing-cactus.dataset.actual_load` 
    WHERE Time_Stamp >= '{start.strftime("%Y-%m-%d")}'
    AND Time_Stamp < '{end.strftime("%Y-%m-%d")}'
    """

    load_df = pandas_gbq.read_gbq(sql_1, credentials=credentials)

    sql_2 = f"""
    SELECT Time_Stamp, Name, LBMP____MWHr_ 
    FROM `sipa-adv-c-dancing-cactus.dataset.market_analysis` 
    WHERE Time_Stamp >= '{start.strftime("%Y-%m-%d")}'
    AND Time_Stamp < '{end.strftime("%Y-%m-%d")}'
    """

    lbmp_df = pandas_gbq.read_gbq(sql_2, credentials=credentials)

    load_df["Time_Stamp"] = pd.to_datetime(load_df["Time_Stamp"], errors="coerce")
    lbmp_df["Time_Stamp"] = pd.to_datetime(lbmp_df["Time_Stamp"], errors="coerce")

    merged = pd.merge(
        load_df,
        lbmp_df,
        left_on=["Time_Stamp", "Name"],
        right_on=["Time_Stamp", "Name"],
        how="inner",
    )
    return merged


# ------ Metric functions -------
def get_processed_electricity_data(df: pd.DataFrame, zone: str) -> pd.DataFrame:
    daily_df = (
        df.groupby("Name")
        .resample("D", on="Time_Stamp")["LBMP____MWHr_"]
        .agg(["mean", "max", "min"])
        .reset_index()
    )

    zone_df = daily_df.loc[daily_df["Name"] == zone].copy()
    zone_df = zone_df.sort_values("Time_Stamp")
    return zone_df


def create_comparison_graph(electricity_df: pd.DataFrame, gas_df: pd.DataFrame) -> None:
    base = alt.Chart(electricity_df).encode(alt.X("Time_Stamp").axis(title="Date"))

    area = base.mark_area(opacity=0.3, color="lightblue").encode(
        alt.Y("max").axis(title="LBMP($/MWh)", titleColor="blue"),
        alt.Y2("min"),
    )
    line_electricity = base.mark_line(color="blue").encode(
        alt.Y("mean").axis(title="LBMP($/MWh)", titleColor="blue"),
    )
    electricity_chart = alt.layer(area, line_electricity)
    line_gas = (
        alt.Chart(gas_df)
        .mark_line(color="red")
        .encode(
            alt.X("date"),
            alt.Y("price").axis(title="Natural Gas Price", titleColor="red"),
        )
    )
    return alt.layer(electricity_chart, line_gas).resolve_scale(y="independent")


def create_demand_chart(LBMP_load: pd.DataFrame, selected_zone: str) -> alt.Chart:
    df_filtered = LBMP_load[LBMP_load["Name"] == selected_zone].copy()

    df_filtered["Hour"] = pd.to_datetime(df_filtered["Time_Stamp"]).dt.hour
    df_filtered["time of day"] = pd.cut(
        df_filtered["Hour"],
        bins=[-1, 5, 11, 17, 23],
        labels=["Night", "Morning", "Afternoon", "Evening"],
    )

    points = (
        alt.Chart(df_filtered)
        .mark_circle(opacity=0.4, size=20)
        .encode(
            x=alt.X("Load", title="Load (MW)", scale=alt.Scale(zero=False)),
            y=alt.Y("LBMP____MWHr_", title="Electricity price ($/MWh)"),
            color=alt.Color("time of day:N", title="Time of Day"),
            tooltip=["Time_Stamp", "Load", "LBMP____MWHr_"],
        )
    )

    return points.properties(
        title=f"Load vs. LBMP for {selected_zone} in the selected month"
    )


@st.cache_data
def load_nyiso_geojson():
    geojson_path = os.path.join(PROJECT_ROOT, "data", "geo", "nyiso_zones.geojson")
    if not os.path.exists(geojson_path):
        return None

    with open(geojson_path, "r", encoding="utf-8") as f:
        return json.load(f)


@st.cache_data
def prepare_map_data(lbpm_load: pd.DataFrame) -> pd.DataFrame:
    df = lbpm_load.copy()

    df["Time_Stamp"] = pd.to_datetime(df["Time_Stamp"], errors="coerce")
    df = df.dropna(subset=["Time_Stamp", "Name", "Load", "LBMP____MWHr_"])

    df["zone_code"] = df["Name"].map(MAP_ZONE_MAP)
    df = df.dropna(subset=["zone_code"])

    df["map_date"] = df["Time_Stamp"].dt.date
    df["map_hour"] = df["Time_Stamp"].dt.hour

    grouped = df.groupby(["map_date", "map_hour", "zone_code"], as_index=False).agg(
        avg_load=("Load", "mean"),
        avg_lbmp=("LBMP____MWHr_", "mean"),
    )
    return grouped


def render_zone_map(lbpm_load: pd.DataFrame) -> None:
    st.subheader("NYISO Zone Map")
    st.write(
        "This map shows the average load or average LBMP across NYISO zones "
        "for a selected day and hour in the chosen month."
    )

    geojson_data = load_nyiso_geojson()
    if geojson_data is None:
        st.warning("Map file not found: data/geo/nyiso_zones.geojson")
        return

    map_df = prepare_map_data(lbpm_load)
    if map_df.empty:
        st.warning("No map data available.")
        return

    col1, col2, col3 = st.columns(3)

    available_dates = sorted(map_df["map_date"].unique())
    selected_date = col1.selectbox(
        "Select date for map", available_dates, key="map_date_select"
    )

    selected_hour = col2.slider(
        "Select hour",
        min_value=0,
        max_value=23,
        value=18,
        step=1,
        key="map_hour_slider",
    )

    selected_metric = col3.selectbox(
        "Map metric", ["Average Load", "Average LBMP"], key="map_metric_select"
    )

    filtered = map_df[
        (map_df["map_date"] == selected_date) & (map_df["map_hour"] == selected_hour)
    ].copy()

    if filtered.empty:
        st.info("No zone data available for this date and hour.")
        return

    if selected_metric == "Average Load":
        color_col = "avg_load"
        color_scale = "Blues"
        legend_title = "Load (MW)"
    else:
        color_col = "avg_lbmp"
        color_scale = "Reds"
        legend_title = "LBMP ($/MWh)"

    fig = px.choropleth_mapbox(
        filtered,
        geojson=geojson_data,
        locations="zone_code",
        featureidkey="properties.Zone",
        color=color_col,
        color_continuous_scale=color_scale,
        mapbox_style="carto-darkmatter",
        center={"lat": 42.9, "lon": -75.5},
        zoom=5.5,
        opacity=0.72,
        hover_name="zone_code",
        hover_data={
            "avg_load": ":.1f",
            "avg_lbmp": ":.2f",
            "zone_code": False,
        },
        title=f"{selected_metric} by NYISO Zone | {selected_date} {selected_hour:02d}:00",
    )

    fig.update_layout(
        height=620,
        margin={"r": 0, "t": 60, "l": 0, "b": 0},
        coloraxis_colorbar=dict(title=legend_title),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )

    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(
        filtered.sort_values(color_col, ascending=False).reset_index(drop=True),
        use_container_width=True,
    )


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
def graph_legend() -> str:
    return (
        "In the comparison graph, the blue line represents the average daily LBMP for the selected NYISO zone, "
        "while the shaded area shows the range between daily minimum and maximum LBMP. The red line represents "
        "the Henry Hub natural gas price for the same period. The two y-axes are independent to allow for clearer visualization of both series."
    )


def demand_interpretation(df: pd.DataFrame, zone: str) -> str:
    avg_load = df["Load"].mean()
    avg_lbmp = df["LBMP____MWHr_"].mean()
    max_load = df["Load"].max()

    return (
        f"In {zone}, the average load for the selected month is {avg_load:,.0f} MW, "
        f"with an average electricity price of ${avg_lbmp:.2f}/MWh. "
        f"The scatter plot shows a positive relationship between demand and price — "
        f"as load approaches its peak of {max_load:,.0f} MW, prices tend to rise and become more volatile. "
        f"This reflects the nonlinear nature of electricity markets, where generators with higher marginal costs "
        f"are dispatched during peak demand periods."
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
        - Henry Hub natural gas prices
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


def render_demand_section(year: int, month: int) -> None:
    st.header("Electricity Price vs. Load")
    st.write("""        
        This section explores the relationship between electricity prices and demand (load) in the selected NYISO zone and month. 
        The scatter plot shows how LBMP varies with load, colored by time of day.
        """)

    with st.spinner("Loading demand data..."):
        LBMP_load = merge_load_and_lbmp(year, month)

    selected_zone = st.selectbox(
        "Select a NYISO zone",
        options=list(ZONE_MAP.keys()),
        format_func=lambda x: ZONE_MAP.get(x),
        index=list(ZONE_MAP.keys()).index("N.Y.C."),
        key="demand_zone_select",
    )
    chart = create_demand_chart(LBMP_load, selected_zone)
    st.altair_chart(chart, use_container_width=True)

    st.write("**Interpretation**")
    st.write(demand_interpretation(LBMP_load, selected_zone))

    st.divider()
    render_zone_map(LBMP_load)
    st.divider()


def render_electricity_section(year: int, month: int) -> None:
    st.header("The Comparison of Electricity and Gas Markets")

    st.write(
        """
        This section explores the relationship between NYISO real-time electricity prices and Henry Hub natural gas prices. Select a zone to examine
        how locational marginal prices vary over the available monthly sample.
        """
    )

    # input month and zone
    selected_month = datetime.date(year, month, 1)
    selected_month_str = selected_month.strftime("%Y-%m-%d")

    if selected_month > datetime.date.today():
        st.error("No data available.")
        st.stop()

    try:
        realtime_df = load_nyiso_realtime(selected_month_str)
        selected_zone = st.selectbox(
            "Select a NYISO zone",
            options=list(ZONE_MAP.keys()),
            format_func=lambda x: ZONE_MAP.get(x),
            index=list(ZONE_MAP.keys()).index("N.Y.C."),
            key="comparison_zone_select",
        )

        zone_df = get_processed_electricity_data(realtime_df, selected_zone)

        gas_df = load_henry_hub_data()
        filtered = gas_df[
            (gas_df["date"].dt.year == year) & (gas_df["date"].dt.month == month)
        ]

        chart = create_comparison_graph(zone_df, filtered)
        st.altair_chart(chart, use_container_width=True)

    except Exception as exc:
        st.error(
            f"Failed to load NYISO electricity data from online public source: {exc}"
        )
        return

    if zone_df.empty:
        st.warning("No electricity data available for the selected zone.")
        return

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Peak LBMP", f"${zone_df['max'].max():.2f}")
    with col2:
        st.metric("Avg LBMP", f"${zone_df['mean'].mean():.2f}")

    st.caption(graph_legend())

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
    render_intro()

    year = st.selectbox(
        "Year for electricity data", range(2017, 2027), index=9, key="global_year"
    )
    month = st.selectbox("Month for electricity data", range(1, 13), key="global_month")

    render_demand_section(year, month)

    render_electricity_section(year, month)

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
