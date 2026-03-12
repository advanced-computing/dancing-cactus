from proposal import get_zip
from proposal import get_csv
from proposal import process_nyiso_data
from datetime import date
import pandas as pd


def test_get_zip():
    test_date_1 = date(2025, 12, 30)
    expected_1 = (
        "https://mis.nyiso.com/public/csv/realtime/20251201realtime_zone_csv.zip"
    )
    assert get_zip(test_date_1) == expected_1

    test_date_2 = date(2025, 12, 1)
    expected_2 = (
        "https://mis.nyiso.com/public/csv/realtime/20251201realtime_zone_csv.zip"
    )
    assert get_zip(test_date_2) == expected_2


def test_get_csv():
    test_date_1 = date(2025, 12, 30)
    expected_1 = "20251230realtime_zone.csv"
    assert get_csv(test_date_1) == expected_1

    test_date_2 = date(2025, 12, 1)
    expected_2 = "20251201realtime_zone.csv"
    assert get_csv(test_date_2) == expected_2


def test_process_nyiso_data():
    dummy_data = pd.DataFrame(
        {
            "Name": ["N.Y.C.", "HUDSON VL", "N.Y.C."],
            "Time Stamp": [
                "02/01/2026 00:00:00",
                "02/01/2026 00:00:00",
                "02/01/2026 01:00:00",
            ],
            "LBMP ($/MWHr)": [30.5, 40.2, 35.0],
        }
    )

    result = process_nyiso_data(dummy_data, "N.Y.C.")
    assert len(result) == 2

    assert "HUDSON VL" not in result["Name"].values

    assert pd.api.types.is_datetime64_any_dtype(result["Time Stamp"])
