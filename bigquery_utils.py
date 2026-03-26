import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery


@st.cache_data
def load_henry_hub_from_bigquery():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )

    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )

    query = """
        SELECT date, series_description, price
        FROM `sipa-adv-c-dancing-cactus.dataset.henry_hub_prices`
        ORDER BY date
    """

    df = client.query(query).to_dataframe()
    return df
