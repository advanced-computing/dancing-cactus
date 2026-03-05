from __future__ import annotations


from data_validation import (
    load_and_validate_pal,
    load_and_validate_realtime,
)


def test_pal_validation_runs():
    df = load_and_validate_pal()
    assert len(df) > 0


def test_realtime_validation_runs():
    df = load_and_validate_realtime()
    assert len(df) > 0
