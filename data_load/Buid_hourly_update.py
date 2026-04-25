import os
import json

from google.cloud import bigquery
from google.oauth2 import service_account

table_id = "dataset.market_analysis"
project_id = "sipa-adv-c-dancing-cactus"


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


def get_latest_date(credentials):
    client = bigquery.Client(credentials=credentials, project=project_id)
    query = """
    SELECT MAX(hourly_time_stamp) 
    FROM `sipa-adv-c-dancing-cactus.dataset.hourly_lbmp`
    """
    client_job = client.query(query)
    results = client_job.result()
    latest_date = list(results)[0][0]
    return latest_date


def add_latest_data_to_hourly_table(credentials):
    client = bigquery.Client(credentials=credentials, project=project_id)
    query = """
    INSERT INTO `sipa-adv-c-dancing-cactus.dataset.hourly_lbmp` (hourly_time_stamp, Year, Month, Day, Hour, Name, LBMP)
    SELECT 
    DATETIME_TRUNC(Time_Stamp, HOUR) AS hourly_time_stamp,
    EXTRACT(YEAR FROM Time_Stamp) AS Year,
    EXTRACT(MONTH FROM Time_Stamp) AS Month,
    EXTRACT(DAY FROM Time_Stamp) AS Day,
    EXTRACT(HOUR FROM Time_Stamp) AS Hour,
    Name,
    AVG(LBMP____MWHr_) AS LBMP
    FROM `sipa-adv-c-dancing-cactus.dataset.market_analysis`
    WHERE Time_Stamp > (
        SELECT MAX(hourly_time_stamp) 
        FROM `sipa-adv-c-dancing-cactus.dataset.hourly_lbmp`
    )
    GROUP BY hourly_time_stamp, Year, Month, Day, Hour, Name
    """
    client_job = client.query(query)
    results = client_job.result()
    return results


def main() -> None:
    credentials = authenticatation()
    add_latest_data_to_hourly_table(credentials)


if __name__ == "__main__":
    main()
