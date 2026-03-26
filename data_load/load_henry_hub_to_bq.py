import streamlit as st
import requests
import pandas as pd
from google.cloud import bigquery
from pandas_gbq import to_gbq

PROJECT_ID = "sipa-adv-c-dancing-cactus"
DATASET_ID = "dataset"
TABLE_ID = "henry_hub_prices"
TABLE_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


def fetch_henry_hub_data():
    api_key = st.secrets["EIA_API_KEY"]

    url = (
        "https://api.eia.gov/v2/natural-gas/pri/fut/data/"
        "?frequency=daily"
        "&data[0]=value"
        "&start=1993-12-24"
        "&sort[0][column]=period"
        "&sort[0][direction]=desc"
        f"&api_key={api_key}"
    )

    response = requests.get(url, timeout=30)
    response.raise_for_status()
    raw = response.json()

    records = raw["response"]["data"]
    df = pd.DataFrame(records)

    print("Columns from API:", df.columns.tolist())

    keep_cols = [
        c for c in ["period", "series-description", "value"] if c in df.columns
    ]
    df = df[keep_cols].copy()

    df = df.rename(
        columns={
            "period": "date",
            "series-description": "series_description",
            "value": "price",
        }
    )

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    if "series_description" not in df.columns:
        df["series_description"] = "Henry Hub Natural Gas Price"

    df = df.dropna(subset=["date", "price"]).drop_duplicates()

    return df


def ensure_table_exists():
    client = bigquery.Client(project=PROJECT_ID)

    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    client.get_dataset(dataset_ref)

    schema = [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("series_description", "STRING"),
        bigquery.SchemaField("price", "FLOAT"),
    ]

    try:
        client.get_table(TABLE_FULL_ID)
        print(f"Table already exists: {TABLE_FULL_ID}")
    except Exception:
        table = bigquery.Table(TABLE_FULL_ID, schema=schema)
        client.create_table(table)
        print(f"Created table: {TABLE_FULL_ID}")


def upload_to_bigquery(df):
    to_gbq(
        dataframe=df,
        destination_table=f"{DATASET_ID}.{TABLE_ID}",
        project_id=PROJECT_ID,
        if_exists="replace",
    )
    print(f"Uploaded {len(df)} rows to {TABLE_FULL_ID}")


def main():
    df = fetch_henry_hub_data()

    print(df.head())
    print(df.dtypes)
    print(f"Row count: {len(df)}")

    ensure_table_exists()
    upload_to_bigquery(df)


if __name__ == "__main__":
    main()
