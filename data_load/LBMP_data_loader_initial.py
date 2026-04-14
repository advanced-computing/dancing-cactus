import json
import os

import requests
from io import BytesIO
from zipfile import ZipFile

import pandas as pd
import pandas_gbq
from google.oauth2 import service_account

from datetime import date


def authenticatation() -> any:
    SCOPES = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/drive",
    ]

    bq_credentials = os.environ["LBMP_DATA"]
    bq_credentials = json.loads(bq_credentials)
    credentials = service_account.Credentials.from_service_account_info(
        bq_credentials, scopes=SCOPES
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


def create_dataset(df: pd.DataFrame, credentials: any, mode: str) -> None:
    table_id = "dataset.market_analysis"
    project_id = "sipa-adv-c-dancing-cactus"

    df = pandas_gbq.to_gbq(
        df,
        table_id,
        project_id=project_id,
        credentials=credentials,
        if_exists=mode,
    )


def main() -> None:
    period = pd.date_range(
        start="2017/01",
        end=date.today(),
        freq="MS",
    )
    target_month = period.strftime("%Y%m")

    credentials = authenticatation()

    for i, month in enumerate(target_month):
        df = nys_realtime_data(month)
        df_clean = clean_colum_name(df)
        if i == 0:
            mode = "replace"
        else:
            mode = "append"
        create_dataset(df_clean, credentials, mode)


if __name__ == "__main__":
    main()
