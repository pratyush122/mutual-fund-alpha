"""
Unit tests for factor model module
"""

import pytest
import pandas as pd
import numpy as np
import sys
import os

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'mutual_fund_alpha'))

from src.analysis.factor_model import run_factor_regression, FactorResult

def test_run_factor_regression():
    """Test the factor regression function."""
    # Create mock data
    dates = pd.date_range('2023-01-01', periods=100, freq='D')
    excess_returns = pd.Series(np.random.randn(100), index=dates)

    factors = pd.DataFrame({
        'mkt_rf': np.random.randn(100),
        'smb': np.random.randn(100),
        'hml': np.random.randn(100)
    }, index=dates)

    # Run regression
    result = run_factor_regression(excess_returns, factors)

    # Check result type
    assert isinstance(result, FactorResult)

    # Check that all attributes exist
    assert hasattr(result, 'alpha')
    assert hasattr(result, 'beta_mkt')
    assert hasattr(result, 'beta_smb')
    assert hasattr(result, 'beta_hml')
    assert hasattr(result, 'r_squared')
    assert hasattr(result, 't_stat_alpha')
    assert hasattr(result, 'p_value_alpha')
    assert hasattr(result, 'residual_std_err')
    assert hasattr(result, 'n_observations')

def test_run_factor_regression_insufficient_data():
    """Test factor regression with insufficient data."""
    # Create mock data with insufficient observations
    dates = pd.date_range('2023-01-01', periods=5, freq='D')
    excess_returns = pd.Series(np.random.randn(5), index=dates)

    factors = pd.DataFrame({
        'mkt_rf': np.random.randn(5),
        'smb': np.random.randn(5),
        'hml': np.random.randn(5)
    }, index=dates)

    # Run regression
    result = run_factor_regression(excess_returns, factors)

    # Check that we get a result even with insufficient data
    assert isinstance(result, FactorResult)
    assert result.n_observations == 5

if __name__ == "__main__":
    pytest.main([__file__])