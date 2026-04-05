"""
Database Table Setup Script
"""

import os
import json
from supabase import create_client
from src.utils.logger import logger
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_tables() -> None:
    """Create database tables using Supabase client."""
    logger.info("Creating database tables...")

    try:
        # Get Supabase credentials
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")

        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment variables")

        # Create Supabase client
        client = create_client(url, key)

        # For Supabase, we need to use the REST API to create tables
        # This is a workaround since we can't run raw SQL directly
        # We'll create tables by inserting a dummy record and then deleting it

        logger.info("Tables should be created manually in Supabase dashboard:")
        logger.info("1. funds table with columns: scheme_code (TEXT, PK), scheme_name (TEXT), category (TEXT), aum_cr (REAL), inception_date (DATE)")
        logger.info("2. nav_history table with columns: id (INTEGER, PK), scheme_code (TEXT, FK), date (DATE), nav (REAL), daily_return (REAL)")
        logger.info("3. factor_data table with columns: date (DATE, PK), mkt_rf (REAL), smb (REAL), hml (REAL), rf (REAL)")
        logger.info("4. alpha_results table with columns: id (INTEGER, PK), scheme_code (TEXT, FK), computed_at (TIMESTAMP), window_days (INTEGER), alpha (REAL), beta_mkt (REAL), beta_smb (REAL), beta_hml (REAL), r_squared (REAL), t_stat_alpha (REAL), p_value (REAL), sharpe (REAL), info_ratio (REAL), skill_score (REAL), percentile_rank (REAL), alpha_persistence (REAL), factor_exposures (JSON), rolling_windows (JSON)")

        # Alternative: Try to create tables by defining them through the API
        # This requires the tables to already exist in Supabase, which they don't

        logger.info("Please create the tables in Supabase dashboard first, then run the seeding script.")

    except Exception as e:
        logger.error(f"Failed to create tables: {e}")
        raise

if __name__ == "__main__":
    create_tables()