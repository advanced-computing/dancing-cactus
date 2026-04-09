import requests
from io import BytesIO
from zipfile import ZipFile

import pandas as pd
import pandas_gbq
import pydata_google_auth

from datetime import date

from google.cloud import bigquery

table_id = "dataset.market_analysis"
project_id = "sipa-adv-c-dancing-cactus"


def authenticatation() -> any:
    SCOPES = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/drive",
    ]

    credentials = pydata_google_auth.get_user_credentials(
        SCOPES,
        auth_local_webserver=True,
    )
    return credentials


def nys_realtime_data(target_month: str) -> pd.DataFrame:
    NYISO_BASE_URL = f"https://mis.nyiso.com/public/csv/realtime/{target_month}01realtime_zone_csv.zip"
    response = requests.get(NYISO_BASE_URL)
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
    return combined


def clean_colum_name(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.replace(r"[^a-zA-Z0-9]", "_", regex=True)

    if "Time_Stamp" in df.columns:
        df["Time_Stamp"] = pd.to_datetime(df["Time_Stamp"])

    return df


def get_latest_date(credentials):
    client = bigquery.Client(credentials=authenticatation(), project=project_id)
    query = """
    SELECT MAX(Time_Stamp)
    FROM `sipa-adv-c-dancing-cactus.dataset.market_analysis` 
    """
    client_job = client.query(query)
    results = client_job.result()
    latest_date = list(results)[0][0]
    return latest_date


def add_dataset(df: pd.DataFrame, credentials: any) -> None:
    df = pandas_gbq.to_gbq(
        df,
        table_id,
        project_id=project_id,
        credentials=credentials,
        if_exists="append",
    )


def main() -> None:
    credentials = authenticatation()
    last_dt = get_latest_date(credentials)
    period = pd.date_range(
        start=last_dt.strftime("%Y-%m-01"),
        end=date.today(),
        freq="MS",
    )
    target_month = period.strftime("%Y%m")

    for i, month in enumerate(target_month):
        df = nys_realtime_data(month)
        df_clean = clean_colum_name(df)
        df_clean_latest = df_clean[df_clean["Time_Stamp"] > last_dt]
        add_dataset(df_clean_latest, credentials)


if __name__ == "__main__":
    main()
