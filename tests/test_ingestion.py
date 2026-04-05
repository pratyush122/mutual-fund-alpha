"""
Unit tests for data ingestion modules
"""

import pytest
import pandas as pd
import sys
import os

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "mutual_fund_alpha"))

from src.ingestion.mock_data import (
    generate_mock_amfi_nav_data,
    generate_mock_fama_french_factors,
)


def test_generate_mock_amfi_nav_data():
    """Test mock AMFI NAV data generation."""
    # Generate mock data
    df = generate_mock_amfi_nav_data(n_funds=5, days=30)

    # Check basic properties
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert "scheme_code" in df.columns
    assert "scheme_name" in df.columns
    assert "date" in df.columns
    assert "nav" in df.columns

    # Check number of funds
    assert df["scheme_code"].nunique() == 5

    # Check that NAV values are positive
    assert (df["nav"] > 0).all()


def test_generate_mock_fama_french_factors():
    """Test mock Fama-French factor data generation."""
    # Generate mock data
    df = generate_mock_fama_french_factors(days=30)

    # Check basic properties
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert "date" in df.columns
    assert "mkt_rf" in df.columns
    assert "smb" in df.columns
    assert "hml" in df.columns
    assert "rf" in df.columns

    # Check that we have the expected number of records (business days)
    assert len(df) <= 30  # Could be less due to weekends/holidays


def test_mock_data_consistency():
    """Test that mock data generation is consistent."""
    # Generate data twice with same parameters
    df1 = generate_mock_amfi_nav_data(n_funds=3, days=20)
    df2 = generate_mock_amfi_nav_data(n_funds=3, days=20)

    # Check that structure is the same
    assert df1.shape == df2.shape
    assert list(df1.columns) == list(df2.columns)


if __name__ == "__main__":
    pytest.main([__file__])
