from __future__ import annotations

import datetime

import pandas as pd
import streamlit as st
import altair as alt

from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud.bigquery import Client

import json
import plotly.express as px

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from src.bigquery_utils import load_henry_hub_from_bigquery  # noqa: E402

# ------ Page config ------
st.set_page_config(page_title="Energy Market Analysis", layout="wide")


# ------ Constants ------
ZONE_INFO = {
    "WEST": {"code": "A", "label": "A - West (Buffalo/Niagara)"},
    "GENESE": {"code": "B", "label": "B - Genesee (Rochester)"},
    "CENTRL": {"code": "C", "label": "C - Central (Syracuse)"},
    "NORTH": {"code": "D", "label": "D - North (St. Lawrence)"},
    "MHK VL": {"code": "E", "label": "E - Mohawk Valley"},
    "CAPITL": {"code": "F", "label": "F - Capital (Albany)"},
    "HUD VL": {"code": "G", "label": "G - Hudson Valley"},
    "MILLWD": {"code": "H", "label": "H - Millwood"},
    "DUNWOD": {"code": "I", "label": "I - Dunwoodie"},
    "N.Y.C.": {"code": "J", "label": "J - New York City"},
    "LONGIL": {"code": "K", "label": "K - Long Island"},
}

today = datetime.date.today()
latest_month = today.strftime("%Y-%m")
time_period = (
    pd.date_range(start="2017-01", end=latest_month, freq="MS")
    .strftime("%Y-%m")
    .tolist()
)
one_month_ago = (today.replace(day=1) - datetime.timedelta(days=1)).strftime("%Y-%m")

# ------ Credentials ------
creds = st.secrets["gcp_service_account"]
credentials = service_account.Credentials.from_service_account_info(creds)


# ------ Data loaders -------
@st.cache_resource
def get_bq_client() -> Client:
    return bigquery.Client(credentials=credentials, project="sipa-adv-c-dancing-cactus")


@st.cache_data(ttl=3600)
def load_lbmp(month: list) -> pd.DataFrame:
    client = get_bq_client()
    month_list = ",".join(f"'{m}'" for m in month)

    sql = f"""
    SELECT hourly_time_stamp, Name, LBMP 
    FROM `sipa-adv-c-dancing-cactus.dataset.hourly_lbmp` 
    WHERE FORMAT_DATE('%Y-%m', hourly_time_stamp) IN ({month_list})
    """
    df = client.query(sql).to_dataframe()
    df["hourly_time_stamp"] = pd.to_datetime(df["hourly_time_stamp"], errors="coerce")
    return df


@st.cache_data(ttl=3600)
def load_actual_load(month: list) -> pd.DataFrame:
    client = get_bq_client()
    month_list = ",".join(f"'{m}'" for m in month)

    sql = f"""
    SELECT Time_Stamp, Name, Load 
    FROM `sipa-adv-c-dancing-cactus.dataset.actual_load` 
    WHERE FORMAT_DATE('%Y-%m', Time_Stamp) IN ({month_list})
    """
    df = client.query(sql).to_dataframe()
    df["Time_Stamp"] = pd.to_datetime(df["Time_Stamp"], errors="coerce")
    return df


@st.cache_data(ttl=3600)
def merge_load_and_lbmp(month: list) -> pd.DataFrame:
    load_df = load_actual_load(month)
    lbmp_df = load_lbmp(month)

    merged = pd.merge(
        load_df,
        lbmp_df,
        left_on=["Time_Stamp", "Name"],
        right_on=["hourly_time_stamp", "Name"],
        how="inner",
    )
    return merged


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
    df = df.dropna(subset=["Time_Stamp", "Name", "Load", "LBMP"])

    df["zone_code"] = df["Name"].map(
        lambda x: ZONE_INFO[x]["code"] if x in ZONE_INFO else None
    )
    df["zone_label"] = df["Name"].map(
        lambda x: ZONE_INFO[x]["label"] if x in ZONE_INFO else None
    )
    df = df.dropna(subset=["zone_code"])

    df["map_date"] = df["Time_Stamp"].dt.date
    df["map_hour"] = df["Time_Stamp"].dt.hour

    grouped = df.groupby(
        ["map_date", "map_hour", "zone_code", "zone_label"], as_index=False
    ).agg(
        avg_load=("Load", "mean"),
        avg_lbmp=("LBMP", "mean"),
    )
    return grouped


def make_zone_choropleth(
    filtered: pd.DataFrame,
    geojson_data: dict,
    color_col: str,
    title: str,
    color_scale: str,
    legend_title: str,
):
    fig = px.choropleth_mapbox(
        filtered,
        geojson=geojson_data,
        locations="zone_code",
        featureidkey="properties.Zone",
        color=color_col,
        color_continuous_scale=color_scale,
        mapbox_style="carto-positron",
        center={"lat": 42.5, "lon": -75.5},
        zoom=4.5,
        opacity=0.72,
        hover_name="zone_label",
        hover_data={
            "avg_load": ":.1f",
            "avg_lbmp": ":.2f",
            "zone_code": False,
            "zone_label": False,
        },
        title=title,
    )

    fig.update_layout(
        height=520,
        margin={"r": 0, "t": 45, "l": 0, "b": 0},
        coloraxis_colorbar=dict(title=legend_title),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )

    return fig


# ------ Metric functions -------
def get_processed_electricity_data(df: pd.DataFrame, zone: str) -> pd.DataFrame:
    daily_df = (
        df.groupby("Name")
        .resample("D", on="hourly_time_stamp")["LBMP"]
        .agg(["mean", "max", "min"])
        .reset_index()
    )

    zone_df = daily_df.loc[daily_df["Name"] == zone].copy()
    zone_df = zone_df.sort_values("hourly_time_stamp")
    return zone_df


def create_demand_scatter_plot(
    LBMP_load: pd.DataFrame, selected_zone: str
) -> alt.Chart:
    df_filtered = LBMP_load[LBMP_load["Name"] == selected_zone].copy()

    df_filtered["Hour"] = pd.to_datetime(df_filtered["Time_Stamp"]).dt.hour
    df_filtered["time of day"] = pd.cut(
        df_filtered["Hour"],
        bins=[-1, 5, 11, 17, 23],
        labels=["Night", "Morning", "Afternoon", "Evening"],
    )

    df_filtered["month"] = (
        pd.to_datetime(df_filtered["Time_Stamp"]).dt.to_period("M").astype(str)
    )

    points = (
        alt.Chart(df_filtered)
        .mark_circle(opacity=0.4, size=20)
        .encode(
            x=alt.X("Load", title="Load (MW)", scale=alt.Scale(zero=False)),
            y=alt.Y("LBMP", title="Electricity price ($/MWh)"),
            color=alt.Color("time of day:N", title="Time of Day"),
            tooltip=["Time_Stamp", "Load", "LBMP"],
        )
        .facet("month:N", columns=3)
    )

    return points.properties(
        title=f"Load vs. LBMP for {selected_zone} in the selected month"
    )


def create_comparison_graph(electricity_df: pd.DataFrame, gas_df: pd.DataFrame) -> None:
    base = alt.Chart(electricity_df).encode(
        alt.X("hourly_time_stamp").axis(title="Date")
    )

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


def render_zone_map(lbpm_load: pd.DataFrame) -> None:
    st.write(
        "These two maps compare average load and average LBMP across NYISO zones "
        "for the same selected day and hour."
        "Notice where the two patterns align — and where they diverge. "
    )
    geojson_data = load_nyiso_geojson()
    if geojson_data is None:
        st.warning("Map file not found: data/geo/nyiso_zones.geojson")
        return

    map_df = prepare_map_data(lbpm_load)
    if map_df.empty:
        st.warning("No map data available.")
        return

    col1, col2 = st.columns(2)

    available_dates = sorted(map_df["map_date"].unique())

    selected_date = col1.selectbox(
        "Select date for maps",
        available_dates,
        index=len(available_dates) - 1,
        key="map_date_select",
    )

    selected_hour = col2.slider(
        "Select hour",
        min_value=0,
        max_value=23,
        value=18,
        step=1,
        key="map_hour_slider",
    )

    filtered = map_df[
        (map_df["map_date"] == selected_date) & (map_df["map_hour"] == selected_hour)
    ].copy()

    if filtered.empty:
        st.info("No zone data available for this date and hour.")
        return

    left_map, right_map = st.columns(2)

    load_fig = make_zone_choropleth(
        filtered=filtered,
        geojson_data=geojson_data,
        color_col="avg_load",
        title=f"Average Load by NYISO Zone | {selected_date} {selected_hour:02d}:00",
        color_scale="Blues",
        legend_title="Load (MW)",
    )

    price_fig = make_zone_choropleth(
        filtered=filtered,
        geojson_data=geojson_data,
        color_col="avg_lbmp",
        title=f"Average LBMP by NYISO Zone | {selected_date} {selected_hour:02d}:00",
        color_scale="Reds",
        legend_title="LBMP ($/MWh)",
    )

    with left_map:
        st.plotly_chart(load_fig, use_container_width=True)

    with right_map:
        st.plotly_chart(price_fig, use_container_width=True)

    with st.expander("Preview zone-level map data"):
        display_df = filtered.sort_values("avg_load", ascending=False).reset_index(
            drop=True
        )
        display_df = display_df.drop(columns=["zone_code"])
        display_df = display_df.rename(columns={"zone_label": "Zone"})
        st.dataframe(
            display_df,
            use_container_width=True,
        )

    st.info(
        "Key takeaway: Electricity prices are shaped not only by demand levels but by where that demand occurs within the grid."
    )


# ------ Interpretation text ------
def graph_legend() -> str:
    return (
        "In the comparison graph, the blue line represents the average daily LBMP for the selected NYISO zone, "
        "while the shaded area shows the range between daily minimum and maximum LBMP. The red line represents "
        "the Henry Hub natural gas price for the same period. The two y-axes are independent to allow for clearer visualization of both series."
    )


# ------ Render sections ------
def render_sidebar() -> None:
    st.sidebar.title("Energy Market Dashboard")
    st.sidebar.markdown(
        """
        **What you'll find here**
        - Spatial comparison of demand and price across NYISO zones
        - Hourly load–price dynamics within a selected zone
        - LBMP vs. Henry Hub natural gas benchmark

        **Data sources**
        - NYISO real-time LBMP and actual load
        - Henry Hub natural gas spot price (EIA)

        Data is refreshed daily via an automated pipeline into BigQuery.
        """
    )


def render_intro() -> None:
    st.title("🏬 NY State Electricity Prices vs Demand and Supply")
    st.write(
        """
        This page combines two key datasets to explore the dynamics of electricity prices in New York State:
        1. NYISO real-time electricity prices (LBMP) and load data, which provide granular insights into how prices fluctuate in response to demand and supply conditions across different zones and times.
        2. Henry Hub natural gas prices, which serve as a benchmark for fuel costs that often influence electricity prices, especially in a gas-heavy generation mix like New York's.
        """
    )
    st.divider()


def render_demand_section(month: int) -> None:
    st.header("Electricity Price vs. Load")

    with st.spinner("Loading data from BigQuery(this may take a moment)..."):
        LBMP_load = merge_load_and_lbmp(month)

    st.subheader("Spatial Distribution Across NYISO Zones: NYISO Zone Map")

    render_zone_map(LBMP_load)

    st.subheader("Demand-Price Relationship Within a Zone")
    st.write(
        "Zoom into a single zone to see how hourly load and price relate within the selected month."
    )

    selected_zone = st.selectbox(
        "Select a NYISO zone",
        options=list(ZONE_INFO.keys()),
        format_func=lambda x: ZONE_INFO[x]["label"],
        index=list(ZONE_INFO.keys()).index("N.Y.C."),
        key="demand_zone_select",
    )

    chart = create_demand_scatter_plot(LBMP_load, selected_zone)
    st.altair_chart(chart, use_container_width=True)

    st.divider()


def render_electricity_section(month: int) -> None:
    st.header("The Comparison of Electricity and Gas Markets")

    st.write(
        """
        NYISO real-time prices capture local power-market outcomes, while Henry Hub provides a benchmark
        fuel-market signal. Looking at both together helps us place electricity price volatility in a broader
        energy-market context — though this comparison is exploratory rather than a direct causal estimate.

        Select a zone to examine how its locational marginal prices move alongside the Henry Hub benchmark
        over the available monthly sample.
        """
    )

    # input month and zone
    try:
        realtime_df = load_lbmp(month)
        selected_zone = st.selectbox(
            "Select a NYISO zone",
            options=list(ZONE_INFO.keys()),
            format_func=lambda x: ZONE_INFO[x]["label"],
            index=list(ZONE_INFO.keys()).index("N.Y.C."),
            key="comparison_zone_select",
        )

        zone_df = get_processed_electricity_data(realtime_df, selected_zone)

        gas_df = load_henry_hub_data()
        filtered = gas_df[(gas_df["date"].dt.to_period("M").astype(str).isin(month))]

        chart = create_comparison_graph(zone_df, filtered)
        st.altair_chart(chart, use_container_width=True)

        if zone_df.empty:
            st.warning("No electricity data available for the selected zone.")
            return

    except Exception as exc:
        st.error(
            f"Failed to load NYISO electricity data from online public source: {exc}"
        )
        return

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Peak LBMP", f"${zone_df['max'].max():.2f}")
    with col2:
        st.metric("Avg LBMP", f"${zone_df['mean'].mean():.2f}")

    st.caption(graph_legend())

    with st.expander("Preview processed electricity data"):
        st.dataframe(zone_df.head(30), use_container_width=True)

    st.info(
        "Key idea: electricity prices show local short-run market outcomes, "
        "while Henry Hub shows broader benchmark fuel conditions. "
    )

    st.divider()


# ------ Main ------
def main() -> None:
    render_sidebar()
    render_intro()

    reversed_period = time_period[::-1]
    default_start_idx = (
        reversed_period.index(one_month_ago) if one_month_ago in reversed_period else 0
    )

    col_start, col_end = st.columns(2)
    with col_start:
        start_month = st.selectbox(
            "Start Month", options=reversed_period, index=default_start_idx
        )
    with col_end:
        end_month = st.selectbox("End Month", options=reversed_period, index=0)

    start_idx = time_period.index(start_month)
    end_idx = time_period.index(end_month)
    if start_idx < end_idx:
        start_idx, end_idx = end_idx, start_idx
    month = time_period[end_idx : start_idx + 1]

    if not month:
        st.warning("invalid period selected.")
        st.stop()

    if len(month) > 4:
        st.warning("Please select a period of 4 months or less to ensure performance.")
        st.stop()

    render_demand_section(month)

    render_electricity_section(month)


if __name__ == "__main__":
    main()
