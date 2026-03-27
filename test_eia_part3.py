import pandas as pd
import pytest
from eia_part3 import (
    add_week_ending_friday_column,
    build_df_from_eia_data,
    coerce_numeric_and_dropna,
    filter_since,
    latest_value,
    sum_by_week,
    validate_required_columns,
)

# Constants used in tests to satisfy Ruff PLR2004 (no "magic numbers" in comparisons)
EXPECTED_FIRST_VALUE = 100
EXPECTED_FILTERED_VALUE = 2
EXPECTED_LATEST_VALUE = 500.0
EXPECTED_WEEK_COUNT = 2
EXPECTED_WEEK1_SUM = 17
EXPECTED_WEEK2_SUM = 3
EXPECTED_SINGLE_VALUE = 10


def test_build_df_from_eia_data_parses_and_drops_bad_rows():
    # includes: valid row, invalid date, invalid value
    data = [
        {"period": "2012-01-06", "value": "100"},
        {"period": "not-a-date", "value": "200"},
        {"period": "2012-01-13", "value": "not-a-number"},
    ]
    df = build_df_from_eia_data(data)

    # only the first row should survive
    assert len(df) == 1
    assert pd.api.types.is_datetime64_any_dtype(df["week"])
    assert pd.api.types.is_numeric_dtype(df["value"])
    assert df["week"].iloc[0] == pd.to_datetime("2012-01-06")
    assert df["value"].iloc[0] == EXPECTED_FIRST_VALUE


def test_filter_since_keeps_2012_and_after():
    data = [
        {"period": "2011-12-30", "value": "1"},
        {"period": "2012-01-06", "value": "2"},
    ]
    df = build_df_from_eia_data(data)
    df2 = filter_since(df, date_col="week", start_date="2012-01-01")

    assert len(df2) == 1
    assert df2["week"].iloc[0] == pd.to_datetime("2012-01-06")
    assert df2["value"].iloc[0] == EXPECTED_FILTERED_VALUE


def test_latest_value_returns_value_of_most_recent_date_even_if_unsorted():
    # intentionally unsorted
    data = [
        {"period": "2012-01-13", "value": "300"},
        {"period": "2012-01-06", "value": "100"},
        {"period": "2012-01-20", "value": "500"},
    ]
    df = build_df_from_eia_data(data)
    v = latest_value(df, date_col="week", value_col="value")

    assert v == EXPECTED_LATEST_VALUE


def test_latest_value_raises_on_empty_df():
    with pytest.raises(ValueError):
        latest_value(pd.DataFrame(), date_col="week", value_col="value")


def test_sum_by_week_sums_duplicates():
    data = [
        {"period": "2012-01-06", "value": "10"},
        {"period": "2012-01-06", "value": "7"},
        {"period": "2012-01-13", "value": "3"},
    ]
    df = build_df_from_eia_data(data)
    out = sum_by_week(df, date_col="week", value_col="value")

    assert list(out.columns) == ["week", "value"]
    assert len(out) == EXPECTED_WEEK_COUNT
    assert (
        out.loc[out["week"] == pd.to_datetime("2012-01-06"), "value"].iloc[0] == EXPECTED_WEEK1_SUM
    )
    assert (
        out.loc[out["week"] == pd.to_datetime("2012-01-13"), "value"].iloc[0] == EXPECTED_WEEK2_SUM
    )


def test_validate_required_columns_passes_when_present():
    df = pd.DataFrame({"week": [pd.to_datetime("2012-01-06")], "value": [1]})
    validate_required_columns(df, ["week", "value"])  # should not raise


def test_validate_required_columns_raises_when_missing():
    df = pd.DataFrame({"week": [pd.to_datetime("2012-01-06")]})
    with pytest.raises(ValueError):
        validate_required_columns(df, ["week", "value"])


def test_add_week_ending_friday_column_creates_expected_week_ending():
    # Pick a date that is not Friday to make the test obvious
    df = pd.DataFrame({"week": pd.to_datetime(["2012-01-03"])})  # Tuesday
    out = add_week_ending_friday_column(df, date_col="week", new_col="week_ending")

    # Week ending Friday should be 2012-01-06 at midnight
    assert out["week_ending"].iloc[0] == pd.Timestamp("2012-01-06")


def test_coerce_numeric_and_dropna_drops_invalid_values():
    df = pd.DataFrame({"value": ["10", "not-a-number", None]})
    out = coerce_numeric_and_dropna(df, value_col="value")

    assert len(out) == 1
    assert out["value"].iloc[0] == EXPECTED_SINGLE_VALUE
