"""
Corrected Return Computation Module
"""

import pandas as pd
import numpy as np
import os
from typing import Dict, List, Optional
from src.utils.logger import logger

def compute_daily_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute daily log returns for mutual funds.

    Args:
        df: DataFrame with NAV data (columns: scheme_code, scheme_name, date, nav)

    Returns:
        DataFrame with daily returns added
    """
    logger.info("Computing daily log returns...")

    try:
        # Sort by scheme_code and date
        df = df.sort_values(['scheme_code', 'date']).reset_index(drop=True)

        # Compute log returns: ln(NAV_t / NAV_{t-1})
        df['nav_shifted'] = df.groupby('scheme_code')['nav'].shift(1)
        df['daily_return'] = np.log(df['nav'] / df['nav_shifted'])

        # Drop the helper column and NaN values
        df = df.drop(columns=['nav_shifted'])
        df = df.dropna(subset=['daily_return']).reset_index(drop=True)

        logger.info(f"Computed daily returns for {df['scheme_code'].nunique()} funds")
        return df

    except Exception as e:
        logger.error(f"Failed to compute daily returns: {e}")
        raise

def compute_rolling_returns(df: pd.DataFrame, windows: List[int] = [21, 63]) -> pd.DataFrame:
    """
    Compute rolling returns for specified windows.

    Args:
        df: DataFrame with daily returns
        windows: List of window sizes (in days)

    Returns:
        DataFrame with rolling returns added
    """
    logger.info(f"Computing rolling returns for windows: {windows}")

    try:
        # Add rolling return columns for each window
        for window in windows:
            df[f'return_{window}d'] = df.groupby('scheme_code')['daily_return'].transform(
                lambda x: x.rolling(window=window).sum()
            )

        logger.info(f"Computed rolling returns for {len(windows)} windows")
        return df

    except Exception as e:
        logger.error(f"Failed to compute rolling returns: {e}")
        raise

def save_processed_returns(df: pd.DataFrame, output_dir: str = "data/processed/") -> str:
    """
    Save processed returns data to parquet.

    Args:
        df: DataFrame with processed returns
        output_dir: Directory to save the file

    Returns:
        Path to saved file
    """
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "fund_returns.parquet")
    df.to_parquet(output_file, index=False)
    logger.info(f"Saved processed returns to {output_file}")
    return output_file

def main():
    """Main function to process returns."""
    logger.info("Starting return computation pipeline...")

    try:
        # Read mock NAV data
        nav_file = "data/raw/amfi_nav.parquet"
        if not os.path.exists(nav_file):
            logger.error(f"NAV file not found: {nav_file}")
            return

        df = pd.read_parquet(nav_file)
        logger.info(f"Loaded {len(df)} NAV records for {df['scheme_code'].nunique()} funds")

        # Compute daily returns
        df_with_returns = compute_daily_returns(df)

        # Compute rolling returns
        df_with_rolling = compute_rolling_returns(df_with_returns, windows=[21, 63])

        # Save processed data (skip trading calendar alignment for now)
        output_file = save_processed_returns(df_with_rolling)
        logger.info("Return computation pipeline completed successfully")

        print(f"Processed {len(df_with_rolling)} records")
        print(f"Date range: {df_with_rolling['date'].min()} to {df_with_rolling['date'].max()}")
        print(f"Unique funds: {df_with_rolling['scheme_code'].nunique()}")

        return df_with_rolling

    except Exception as e:
        logger.error(f"Return computation pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()