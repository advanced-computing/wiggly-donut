"""Unit tests for Avg_func.average_probabilities."""
import pandas as pd
import pytest
from datetime import datetime

from Avg_func import average_probabilities


# A polymarket/kalshi type DataFrame for tests. 
def _make_poly(timestamps, probabilities):
    """Helper: DataFrame with columns t (datetime), p (float 0-100)."""
    return pd.DataFrame({"t": timestamps, "p": probabilities})


def _make_kalshi(dates, close_cents):
    """Helper: DataFrame with Date (datetime), Close (¢) (float 0-100)."""
    return pd.DataFrame({"Date": dates, "Close (¢)": close_cents})


# Tests 

# Test 1: one date in both sources = one row with the correct average
def test_single_overlapping_day():
    base = datetime(2026, 2, 1)
    poly = _make_poly([base], [40.0])
    kalshi = _make_kalshi([base], [60.0])
    result = average_probabilities(poly, kalshi)
    assert result is not None
    assert len(result) == 1
    EXPECTED_AVG = 50.0
    assert result["Average (%)"].iloc[0] == EXPECTED_AVG

# Test 2: when polymarket has no data -->  function  returns empty result, not crash.
def test_empty_poly():
    base = datetime(2026, 2, 1)
    poly = _make_poly([], [])
    kalshi = _make_kalshi([base], [50.0])
    result = average_probabilities(poly, kalshi)
    assert result is not None
    assert len(result) == 0
    assert list(result.columns) == ["Date", "Average (%)"]

# Test 3: when kalshi has no data -->  function  returns empty result, not crash
def test_empty_kalshi():
    base = datetime(2026, 2, 1)
    poly = _make_poly([base], [50.0])
    kalshi = _make_kalshi([], [])
    result = average_probabilities(poly, kalshi)
    assert result is not None
    assert len(result) == 0

# Test 4: only dates that appear in both sources should be in the result
def test_no_overlapping_dates():
    poly = _make_poly([datetime(2026, 2, 1)], [50.0])
    kalshi = _make_kalshi([datetime(2026, 2, 2)], [50.0])
    result = average_probabilities(poly, kalshi)
    assert result is not None
    assert len(result) == 0

#Test 5: output must have exactly two columns.
def test_output_format():
    base = datetime(2026, 2, 1)
    poly = _make_poly([base], [50.0])
    kalshi = _make_kalshi([base], [50.0])
    result = average_probabilities(poly, kalshi)
    assert result is not None
    assert list(result.columns) == ["Date", "Average (%)"]


