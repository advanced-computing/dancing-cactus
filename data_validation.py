from pathlib import Path
import pandas as pd
from pandera import Check, Column, DataFrameSchema


# Repo-relative paths

REPO_ROOT = Path(__file__).resolve().parent
DATA_DIR = REPO_ROOT / "data"

PAL_CSV = DATA_DIR / "20260211pal.csv"
REALTIME_CSV = DATA_DIR / "20260211realtime_zone.csv"
HENRY_HUB_XLS = DATA_DIR / "RNGWHHDd.xls"

# From your NYISO CSVs
NYISO_TS_FORMAT = "%m/%d/%Y %H:%M:%S"


def _is_parseable_timestamp(series: pd.Series) -> bool:
    # All values must parse with the expected NYISO timestamp format.
    parsed = pd.to_datetime(series, format=NYISO_TS_FORMAT, errors="coerce")
    return parsed.notna().all()


def _non_empty_trimmed(series: pd.Series) -> bool:
    # No blank strings after trimming whitespace.
    return series.astype(str).str.strip().ne("").all()


def _no_duplicate_keys(df: pd.DataFrame, keys: list[str]) -> bool:
    # The key columns should uniquely identify each row.
    return (~df.duplicated(subset=keys)).all()


def _date_is_parseable(series: pd.Series) -> bool:
    # Henry Hub screenshot shows dates like 'Jan 07, 1997'.
    # This checks that pandas can parse all values to datetime.
    parsed = pd.to_datetime(series, errors="coerce")
    return parsed.notna().all()


def _is_strictly_increasing_dates(series: pd.Series) -> bool:
    # Dates should be strictly increasing (no duplicates and sorted).
    s = pd.to_datetime(series, errors="coerce")
    if s.isna().any():
        return False
    return (s.is_monotonic_increasing) and (s.nunique() == len(s))


# ----------------------------
# Schema 1: Load dataset (20260211pal.csv)
# Columns: Time Stamp, Time Zone, Name, PTID, Load
# ----------------------------
LOAD_SCHEMA = DataFrameSchema(
    columns={
        "Time Stamp": Column(
            object,
            checks=[Check(_is_parseable_timestamp, element_wise=False)],
            nullable=False,
        ),
        "Time Zone": Column(
            object,
            checks=[
                Check.isin(["EST", "EDT"]),
                Check(_non_empty_trimmed, element_wise=False),
            ],
            nullable=False,
        ),
        "Name": Column(
            object,
            checks=[
                Check(
                    _non_empty_trimmed,
                    element_wise=False,
                    error="Name cannot be blank.",
                ),
                Check.str_matches(
                    r"^[A-Z0-9 ._-]+$",
                    error="Name should contain only uppercase letters/numbers/spaces and . _ -",
                ),
            ],
            nullable=False,
        ),
        "PTID": Column(
            int,
            checks=[Check.ge(1)],
            nullable=False,
        ),
        "Load": Column(
            float,
            checks=[
                Check.ge(0),
                # sanity bound: catches unit explosions/corrupted values
                Check.le(200000),
            ],
            nullable=False,
        ),
    },
    strict=True,  # For course labs: strict is safer (ensures exact expected columns)
    coerce=True,
    checks=[
        Check(
            lambda df: _no_duplicate_keys(df, ["Time Stamp", "PTID"]),
            element_wise=False,
        )
    ],
)


# ----------------------------
# Schema 2: Real-time price dataset (20260211realtime_zone.csv)
# Columns: Time Stamp, Name, PTID, LBMP ($/MWHr), Marginal Cost Losses ($/MWHr), Marginal Cost Congestion ($/MWHr)
# ----------------------------
PRICE_SCHEMA = DataFrameSchema(
    columns={
        "Time Stamp": Column(
            object,
            checks=[Check(_is_parseable_timestamp, element_wise=False)],
            nullable=False,
        ),
        "Name": Column(
            object,
            checks=[
                Check(
                    _non_empty_trimmed,
                    element_wise=False,
                    error="Name cannot be blank.",
                ),
                Check.str_matches(
                    r"^[A-Z0-9 ._-]+$",
                    error="Name should contain only uppercase letters/numbers/spaces and . _ -",
                ),
            ],
            nullable=False,
        ),
        "PTID": Column(int, checks=[Check.ge(1)], nullable=False),
        "LBMP ($/MWHr)": Column(
            float,
            checks=[Check.ge(-2000), Check.le(20000)],
            nullable=False,
        ),
        "Marginal Cost Losses ($/MWHr)": Column(
            float,
            checks=[Check.ge(-2000), Check.le(20000)],
            nullable=False,
        ),
        "Marginal Cost Congestion ($/MWHr)": Column(
            float,
            checks=[Check.ge(-2000), Check.le(20000)],
            nullable=False,
        ),
    },
    strict=True,
    coerce=True,
    checks=[
        Check(
            lambda df: _no_duplicate_keys(df, ["Time Stamp", "PTID"]),
            element_wise=False,
        )
    ],
)


# ----------------------------
# Schema 3: Henry Hub Natural Gas Spot Price (RNGWHHDd.xls)
# Column A: Date (e.g., "Jan 07, 1997")
# Column B: RNGWHHD (price in $ per Million Btu)
# ----------------------------
HENRY_HUB_SCHEMA = DataFrameSchema(
    columns={
        "Date": Column(
            object,
            checks=[
                Check(_date_is_parseable, element_wise=False),
                Check(_is_strictly_increasing_dates, element_wise=False),
            ],
            nullable=False,
        ),
        "RNGWHHD": Column(
            float,
            checks=[
                Check.ge(0),
                # broad sanity bound for gas prices; catches parsing bugs (e.g., 382 instead of 3.82)
                Check.le(200),
            ],
            nullable=False,
        ),
    },
    strict=True,
    coerce=True,
    checks=[
        # Date should be unique is implied by strictly increasing, but we keep this explicit
        Check(lambda df: _no_duplicate_keys(df, ["Date"]), element_wise=False)
    ],
)


# ----------------------------
# Validators (public API)
# ----------------------------
def validate_load_df(df: pd.DataFrame) -> pd.DataFrame:
    return LOAD_SCHEMA.validate(df, lazy=True)


def validate_price_df(df: pd.DataFrame) -> pd.DataFrame:
    return PRICE_SCHEMA.validate(df, lazy=True)


def validate_henry_hub_df(df: pd.DataFrame) -> pd.DataFrame:
    return HENRY_HUB_SCHEMA.validate(df, lazy=True)


# ----------------------------
# Load + validate (repo-relative, matches your /data folder)
# ----------------------------
def load_and_validate_pal(path: Path = PAL_CSV) -> pd.DataFrame:
    df = pd.read_csv(path)
    return validate_load_df(df)


def load_and_validate_realtime(path: Path = REALTIME_CSV) -> pd.DataFrame:
    df = pd.read_csv(path)
    return validate_price_df(df)
