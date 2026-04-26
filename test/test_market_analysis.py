import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

import pandas as pd  # noqa: E402

from google.cloud import bigquery  # noqa: E402
import streamlit as st  # noqa: E402
from google.oauth2 import service_account  # noqa: E402

from app.market_analysis import load_nyiso_realtime  # noqa: E402
from app.market_analysis import get_processed_electricity_data  # noqa: E402

creds_info = st.secrets["gcp_service_account"]
credentials = service_account.Credentials.from_service_account_info(creds_info)

table_id = "dataset.market_analysis"
project_id = "sipa-adv-c-dancing-cactus"

client = bigquery.Client(credentials=credentials, project=project_id)


def test_load_nyiso_realtime():
    selected_month_1 = ["2025-12"]

    sql_1 = """
    SELECT hourly_time_stamp, Name, LBMP 
    FROM `sipa-adv-c-dancing-cactus.dataset.hourly_lbmp` 
    WHERE FORMAT_DATE('%Y-%m', hourly_time_stamp) = '2025-12'
    """
    client_job = client.query(sql_1)
    expected_df_1 = client_job.to_dataframe()

    selected_month_1_actual = (
        load_nyiso_realtime(selected_month_1)
        .sort_values(by=["hourly_time_stamp", "Name"])
        .reset_index(drop=True)
    )
    expected_df_1_actual = expected_df_1.sort_values(
        by=["hourly_time_stamp", "Name"]
    ).reset_index(drop=True)

    pd.testing.assert_frame_equal(selected_month_1_actual, expected_df_1_actual)

    selected_month_2 = ["2024-05"]

    sql_2 = """
    SELECT hourly_time_stamp, Name, LBMP 
    FROM `sipa-adv-c-dancing-cactus.dataset.hourly_lbmp` 
    WHERE FORMAT_DATE('%Y-%m', hourly_time_stamp) = '2024-05'
    """

    client_job = client.query(sql_2)
    expected_df_2 = client_job.to_dataframe()

    selected_month_2_actual = (
        load_nyiso_realtime(selected_month_2)
        .sort_values(by=["hourly_time_stamp", "Name"])
        .reset_index(drop=True)
    )
    expected_df_2_actual = expected_df_2.sort_values(
        by=["hourly_time_stamp", "Name"]
    ).reset_index(drop=True)

    pd.testing.assert_frame_equal(selected_month_2_actual, expected_df_2_actual)


def test_get_processed_electricity_data():
    test_data = pd.DataFrame(
        {
            "hourly_time_stamp": pd.to_datetime(
                ["2026-01-01 00:00", "2026-01-01 12:00", "2026-01-02 00:00"]
            ),
            "Name": ["Zone A", "Zone A", "Zone A"],
            "LBMP": [10.0, 20.0, 30.0],
        }
    )
    result = get_processed_electricity_data(test_data, "Zone A")

    assert (
        result[result["hourly_time_stamp"] == pd.to_datetime("2026-01-01")][
            "mean"
        ].iloc[0]
        == 15.0
    )
    assert (
        result[result["hourly_time_stamp"] == pd.to_datetime("2026-01-02")][
            "mean"
        ].iloc[0]
        == 30.0
    )
    assert (
        result[result["hourly_time_stamp"] == pd.to_datetime("2026-01-01")]["max"].iloc[
            0
        ]
        == 20.0
    )

    assert len(result.columns) == 5
    assert len(result) == 2
