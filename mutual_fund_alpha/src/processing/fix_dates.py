"""
Date Alignment Fix Module
"""

import pandas as pd
import os
from src.utils.logger import logger

def fix_timestamps_to_dates() -> None:
    """Fix timestamps in mock data to align dates properly."""
    logger.info("Fixing timestamps to align dates...")

    try:
        # Fix fund returns dates
        returns_file = "data/processed/fund_returns.parquet"
        if os.path.exists(returns_file):
            df_returns = pd.read_parquet(returns_file)
            # Convert timestamps to dates only (remove time component)
            df_returns['date'] = pd.to_datetime(df_returns['date']).dt.date
            # Convert back to datetime for consistency
            df_returns['date'] = pd.to_datetime(df_returns['date'])
            # Save back
            df_returns.to_parquet(returns_file, index=False)
            logger.info(f"Fixed dates in returns data: {len(df_returns)} records")

        # Fix factor data dates
        factors_file = "data/raw/fama_french_3factor.parquet"
        if os.path.exists(factors_file):
            df_factors = pd.read_parquet(factors_file)
            # Convert timestamps to dates only (remove time component)
            df_factors['date'] = pd.to_datetime(df_factors['date']).dt.date
            # Convert back to datetime for consistency
            df_factors['date'] = pd.to_datetime(df_factors['date'])
            # Save back
            df_factors.to_parquet(factors_file, index=False)
            logger.info(f"Fixed dates in factors data: {len(df_factors)} records")

        logger.info("Date fixing completed successfully")

    except Exception as e:
        logger.error(f"Failed to fix dates: {e}")
        raise

if __name__ == "__main__":
    fix_timestamps_to_dates()