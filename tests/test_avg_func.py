"""Unit tests for Avg_func.average_probabilities."""
import pandas as pd
import pytest
from datetime import datetime

from Avg_func import average_probabilities


def _make_poly(timestamps, probabilities):
    """Helper: DataFrame with columns t (datetime), p (float 0-100)."""
    return pd.DataFrame({"t": timestamps, "p": probabilities})


def _make_kalshi(dates, close_cents):
    """Helper: DataFrame with Date (datetime), Close (¢) (float 0-100)."""
    return pd.DataFrame({"Date": dates, "Close (¢)": close_cents})


# --- Tests (run before implementing to see them fail) ---


def test_single_overlapping_day_returns_correct_average():
    """One date in both sources: average of the two probabilities."""
    base = datetime(2026, 2, 1)
    poly = _make_poly([base], [40.0])
    kalshi = _make_kalshi([base], [60.0])
    result = average_probabilities(poly, kalshi)
    assert result is not None
    assert len(result) == 1
    assert result["Average (%)"].iloc[0] == 50.0


def test_empty_poly_returns_empty_dataframe():
    """If Polymarket has no rows, inner merge yields no rows."""
    base = datetime(2026, 2, 1)
    poly = _make_poly([], [])
    kalshi = _make_kalshi([base], [50.0])
    result = average_probabilities(poly, kalshi)
    assert result is not None
    assert len(result) == 0
    assert list(result.columns) == ["Date", "Average (%)"]


def test_empty_kalshi_returns_empty_dataframe():
    """If Kalshi has no rows, inner merge yields no rows."""
    base = datetime(2026, 2, 1)
    poly = _make_poly([base], [50.0])
    kalshi = _make_kalshi([], [])
    result = average_probabilities(poly, kalshi)
    assert result is not None
    assert len(result) == 0


def test_no_overlapping_dates_returns_empty():
    """Dates only in one source should not appear (inner join)."""
    poly = _make_poly([datetime(2026, 2, 1)], [50.0])
    kalshi = _make_kalshi([datetime(2026, 2, 2)], [50.0])
    result = average_probabilities(poly, kalshi)
    assert result is not None
    assert len(result) == 0


def test_output_has_only_date_and_average_columns():
    """Returned DataFrame must have exactly columns Date and Average (%)."""
    base = datetime(2026, 2, 1)
    poly = _make_poly([base], [50.0])
    kalshi = _make_kalshi([base], [50.0])
    result = average_probabilities(poly, kalshi)
    assert result is not None
    assert list(result.columns) == ["Date", "Average (%)"]


