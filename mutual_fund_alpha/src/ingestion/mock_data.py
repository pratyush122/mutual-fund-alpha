"""
Mock data generation for testing purposes
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from typing import List, Dict
import random

def generate_mock_amfi_nav_data(n_funds: int = 50, days: int = 365) -> pd.DataFrame:
    """
    Generate mock AMFI NAV data for testing.

    Args:
        n_funds: Number of mock funds to generate
        days: Number of days of historical data

    Returns:
        DataFrame with mock NAV data
    """
    # Generate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    dates = pd.date_range(start=start_date, end=end_date, freq='D')

    # Remove weekends (mutual funds typically don't have NAV on weekends)
    dates = dates[dates.weekday < 5]

    all_data = []

    for i in range(n_funds):
        # Generate fund info
        scheme_code = f"MF{i+1:04d}"
        scheme_name = f"Mock Mutual Fund {i+1}"

        # Generate realistic NAV progression
        # Start with a base NAV between 10 and 100
        base_nav = random.uniform(10, 100)

        # Generate daily returns with some volatility
        daily_returns = np.random.normal(0.0005, 0.02, len(dates))  # Mean return 0.05% daily, 2% volatility

        # Calculate NAV series
        navs = [base_nav]
        for ret in daily_returns[:-1]:  # Exclude last return since we're calculating based on previous NAV
            new_nav = navs[-1] * (1 + ret)
            # Ensure NAV doesn't go too low
            new_nav = max(new_nav, 1.0)
            navs.append(new_nav)

        # Create DataFrame for this fund
        fund_data = pd.DataFrame({
            'scheme_code': scheme_code,
            'scheme_name': scheme_name,
            'date': dates,
            'nav': navs
        })

        all_data.append(fund_data)

    # Combine all funds
    combined_df = pd.concat(all_data, ignore_index=True)

    return combined_df

def generate_mock_fama_french_factors(days: int = 365) -> pd.DataFrame:
    """
    Generate mock Fama-French 3-factor data for testing.

    Args:
        days: Number of days of historical data

    Returns:
        DataFrame with mock factor data
    """
    # Generate date range (business days only)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    dates = pd.date_range(start=start_date, end=end_date, freq='B')  # Business days

    # Generate realistic factor returns
    # Market risk premium (Mkt-RF): Mean ~0.03% daily, volatility ~1%
    mkt_rf = np.random.normal(0.0003, 0.01, len(dates))

    # Size factor (SMB): Mean ~0.01% daily, volatility ~0.8%
    smb = np.random.normal(0.0001, 0.008, len(dates))

    # Value factor (HML): Mean ~0.005% daily, volatility ~0.7%
    hml = np.random.normal(0.00005, 0.007, len(dates))

    # Risk-free rate: Mean ~0.01% daily, low volatility ~0.01%
    rf = np.random.normal(0.0001, 0.0001, len(dates))
    # Ensure positive risk-free rate
    rf = np.abs(rf)

    # Create DataFrame
    factors_df = pd.DataFrame({
        'date': dates,
        'mkt_rf': mkt_rf,
        'smb': smb,
        'hml': hml,
        'rf': rf
    })

    return factors_df

def save_mock_data(output_dir: str = "data/raw/") -> None:
    """
    Generate and save mock data for testing.

    Args:
        output_dir: Directory to save the parquet files
    """
    os.makedirs(output_dir, exist_ok=True)

    # Generate and save mock AMFI NAV data
    print("Generating mock AMFI NAV data...")
    nav_df = generate_mock_amfi_nav_data(n_funds=50, days=365)
    nav_file = os.path.join(output_dir, "amfi_nav.parquet")
    nav_df.to_parquet(nav_file, index=False)
    print(f"Saved mock NAV data to {nav_file}")
    print(f"  - {len(nav_df)} records from {nav_df['scheme_code'].nunique()} funds")

    # Generate and save mock Fama-French factors
    print("Generating mock Fama-French factor data...")
    factors_df = generate_mock_fama_french_factors(days=365)
    factors_file = os.path.join(output_dir, "fama_french_3factor.parquet")
    factors_df.to_parquet(factors_file, index=False)
    print(f"Saved mock Fama-French factors to {factors_file}")
    print(f"  - {len(factors_df)} records")

if __name__ == "__main__":
    save_mock_data()