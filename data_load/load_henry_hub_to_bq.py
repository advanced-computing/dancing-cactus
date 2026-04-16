import json
import os

import streamlit as st
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from pandas_gbq import to_gbq

PROJECT_ID = "sipa-adv-c-dancing-cactus"
DATASET_ID = "dataset"
TABLE_ID = "henry_hub_prices"
TABLE_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


def get_eia_api_key() -> str:
    """
    Return EIA API key from:
    1) GitHub Actions environment variable
    2) Streamlit secrets
    """
    if "EIA_API_KEY" in os.environ:
        return os.environ["EIA_API_KEY"]

    return st.secrets["EIA_API_KEY"]


def get_gcp_credentials():
    """
    Return Google credentials from:
    1) GitHub Actions environment variable GCP_SERVICE_ACCOUNT
    2) Streamlit secrets gcp_service_account
    """
    if "GCP_SERVICE_ACCOUNT" in os.environ:
        service_account_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
    else:
        service_account_info = dict(st.secrets["gcp_service_account"])

    return service_account.Credentials.from_service_account_info(service_account_info)


def fetch_henry_hub_data() -> pd.DataFrame:
    """
    Fetch Henry Hub daily price data from EIA API.
    """
    api_key = get_eia_api_key()

    url = (
        "https://api.eia.gov/v2/natural-gas/pri/fut/data/"
        "?frequency=daily"
        "&data[0]=value"
        "&start=1993-12-24"
        "&sort[0][column]=period"
        "&sort[0][direction]=desc"
        "&offset=0"
        "&length=5000"
        f"&api_key={api_key}"
    )

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    payload = response.json()

    if "response" not in payload or "data" not in payload["response"]:
        raise ValueError("Unexpected EIA API response format.")

    records = payload["response"]["data"]
    if not records:
        raise ValueError("No Henry Hub data returned from EIA API.")

    df = pd.DataFrame(records)

    rename_map = {
        "period": "date",
        "value": "price",
        "series-description": "series_description",
        "seriesDescription": "series_description",
    }
    df = df.rename(columns=rename_map)

    required_columns = ["date", "price"]
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column in API response: {col}")

    if "series_description" not in df.columns:
        df["series_description"] = "Henry Hub Natural Gas Spot Price"

    df = df[["date", "series_description", "price"]].copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    df = df.dropna(subset=["date", "price"]).drop_duplicates()
    df = df.sort_values("date").reset_index(drop=True)

    return df


def ensure_dataset_and_table(credentials) -> None:
    """
    Ensure the BigQuery dataset and table exist.
    """
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset exists: {PROJECT_ID}.{DATASET_ID}")
    except Exception:
        client.create_dataset(dataset_ref)
        print(f"Created dataset: {PROJECT_ID}.{DATASET_ID}")

    schema = [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("series_description", "STRING"),
        bigquery.SchemaField("price", "FLOAT"),
    ]

    try:
        client.get_table(TABLE_FULL_ID)
        print(f"Table exists: {TABLE_FULL_ID}")
    except Exception:
        table = bigquery.Table(TABLE_FULL_ID, schema=schema)
        client.create_table(table)
        print(f"Created table: {TABLE_FULL_ID}")


def upload_to_bigquery(df: pd.DataFrame, credentials) -> None:
    """
    Replace the Henry Hub table in BigQuery with the latest full dataset.
    """
    to_gbq(
        dataframe=df,
        destination_table=f"{DATASET_ID}.{TABLE_ID}",
        project_id=PROJECT_ID,
        credentials=credentials,
        if_exists="replace",
    )
    print(f"Uploaded {len(df)} rows to {TABLE_FULL_ID}")


def main() -> None:
    credentials = get_gcp_credentials()
    df = fetch_henry_hub_data()
    print(f"Fetched {len(df)} rows of Henry Hub data.")
    ensure_dataset_and_table(credentials)
    upload_to_bigquery(df, credentials)
    print("Henry Hub ETL finished successfully.")


if __name__ == "__main__":
    main()
