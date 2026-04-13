# from market_analysis import load_nyiso_realtime
# from market_analysis import load_henry_hub_data
# from market_analysis import compute_electricity_metrics
# from market_analysis import electricity_interpretation
# from market_analysis import gas_interpretation

# import pandas as pd
# import pydata_google_auth
# from google.cloud import bigquery

# SCOPES = [
#     "https://www.googleapis.com/auth/cloud-platform",
#     "https://www.googleapis.com/auth/drive",
# ]

# credentials = pydata_google_auth.get_user_credentials(
#     SCOPES,
#     auth_local_webserver=True,
# )
# client = bigquery.Client(credentials=credentials, project="sipa-adv-c-dancing-cactus")


# def test_load_nyiso_realtime():
#     test_month_1 = "2025-12-01"

#     query = """
#     SELECT Time_Stamp, Name, LBMP____MWHr_
#     FROM `sipa-adv-c-dancing-cactus.dataset.market_analysis`
#     WHERE  Time_Stamp >= "2025-12-01"
#     AND Time_Stamp < "2026-01-01"
#     """
#     client_job = client.query(query)
#     df_1 = client_job.to_dataframe()

#     pd.testing.assert_frame_equal(load_nyiso_realtime(test_month_1), df_1)

#     test_month_2 = "2025-04-01"
#     query = """
#     SELECT Time_Stamp, Name, LBMP____MWHr_
#     FROM `sipa-adv-c-dancing-cactus.dataset.market_analysis`
#     WHERE  Time_Stamp >= "2025-04-01"
#     AND Time_Stamp < "2025-05-01"
#     """
#     client_job = client.query(query)
#     df_2 = client_job.to_dataframe()

#     pd.testing.assert_frame_equal(load_nyiso_realtime(test_month_2), df_2)


# # def test_load_henry_hub_data():
# #     test_date_1 = date(2025, 12, 30)
# #     expected_1 = "20251230realtime_zone.csv"
# #     assert get_csv(test_date_1) == expected_1

# #     test_date_2 = date(2025, 12, 1)
# #     expected_2 = "20251201realtime_zone.csv"
# #     assert get_csv(test_date_2) == expected_2


# def test_compute_electricity_metrics():
#     dummy_data = pd.DataFrame(
#         {
#             "Time_Stamp": pd.to_datetime(
#                 [
#                     "2026-02-01 12:00:00",
#                     "2026-02-02 03:00:00",
#                     "2026-02-05 20:00:00",
#                 ]
#             ),
#             "LBMP____MWHr_": [30.5, 40.2, 35.0],
#         }
#     )
#     result = compute_electricity_metrics(dummy_data)

#     assert result["avg"] == "35.23"
#     assert result["max"] == "40.20"
#     assert result["min"] == "30.50"
#     assert result["peak_hour"] == "2026-02-02 03:00"
