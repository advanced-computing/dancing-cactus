import pandas as pd
from old.page_2 import clean_gas_df


def test_clean_gas_df_none_returns_empty_with_columns():
    out = clean_gas_df(None)
    assert list(out.columns) == ["Date", "Price"]
    assert out.empty


def test_clean_gas_df_empty_df_returns_empty_with_columns():
    out = clean_gas_df(pd.DataFrame())
    assert list(out.columns) == ["Date", "Price"]
    assert out.empty


def test_clean_gas_df_keeps_first_two_columns_renames_and_drops_extra():
    raw = pd.DataFrame(
        {
            "A": ["2026-02-02", "2026-02-01"],
            "B": [2.7, 2.5],
            "C": ["ignore", "ignore"],
        }
    )
    out = clean_gas_df(raw)

    assert list(out.columns) == ["Date", "Price"]
    assert len(out.columns) == 2
    assert len(out) == 2


def test_clean_gas_df_drops_na_rows_in_either_col():
    raw = pd.DataFrame(
        {
            "A": ["2026-02-01", None, "2026-02-03", "2026-02-04"],
            "B": [2.5, 999, None, 2.9],
        }
    )
    out = clean_gas_df(raw)

    # rows with NA in Date or Price should be dropped by df.dropna()
    assert len(out) == 2
    assert out["Price"].tolist() == [2.5, 2.9]


def test_clean_gas_df_coerces_bad_dates_and_drops_them():
    raw = pd.DataFrame(
        {
            "A": ["not-a-date", "2026-02-01"],
            "B": [1.0, 2.0],
        }
    )
    out = clean_gas_df(raw)

    assert len(out) == 1
    assert out["Date"].iloc[0] == pd.Timestamp("2026-02-01")
    assert out["Price"].iloc[0] == 2.0


def test_clean_gas_df_sorts_by_date_ascending():
    raw = pd.DataFrame(
        {
            "A": ["2026-02-03", "2026-02-01", "2026-02-02"],
            "B": [3.0, 1.0, 2.0],
        }
    )
    out = clean_gas_df(raw)

    assert out["Date"].tolist() == [
        pd.Timestamp("2026-02-01"),
        pd.Timestamp("2026-02-02"),
        pd.Timestamp("2026-02-03"),
    ]
    # index should be reset
    assert out.index.tolist() == [0, 1, 2]
