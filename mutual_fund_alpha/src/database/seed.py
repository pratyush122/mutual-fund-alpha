"""
Database Seeding Script
"""

import pandas as pd
import os
from src.database.client import get_db_client, DatabaseClient
from src.utils.logger import logger


def seed_funds_table(client: DatabaseClient) -> None:
    """Seed funds table with mock data."""
    try:
        # Read mock NAV data to get fund information
        nav_file = "data/raw/amfi_nav.parquet"
        if not os.path.exists(nav_file):
            logger.warning(f"NAV file not found: {nav_file}")
            return

        df = pd.read_parquet(nav_file)

        # Extract unique funds
        funds_data = []
        for scheme_code in df["scheme_code"].unique():
            fund_df = df[df["scheme_code"] == scheme_code].iloc[0]
            funds_data.append(
                {
                    "scheme_code": scheme_code,
                    "scheme_name": fund_df["scheme_name"],
                    "category": "Equity",  # Mock category
                    "aum_cr": 1000.0,  # Mock AUM in crores
                    "inception_date": fund_df["date"].min().isoformat(),
                }
            )

        # Upsert funds data
        client.upsert_funds(funds_data)
        logger.info(f"Seeded {len(funds_data)} funds")

    except Exception as e:
        logger.error(f"Failed to seed funds table: {e}")
        raise


def seed_nav_history_table(client: DatabaseClient) -> None:
    """Seed NAV history table with mock data."""
    try:
        # Read mock NAV data
        nav_file = "data/raw/amfi_nav.parquet"
        if not os.path.exists(nav_file):
            logger.warning(f"NAV file not found: {nav_file}")
            return

        df = pd.read_parquet(nav_file)

        # Convert to list of dictionaries
        nav_data = []
        for _, row in df.iterrows():
            nav_data.append(
                {
                    "scheme_code": row["scheme_code"],
                    "date": row["date"].isoformat(),
                    "nav": float(row["nav"]),
                    "daily_return": 0.0,  # Will be calculated later
                }
            )

        # Upsert NAV history data
        client.upsert_nav_history(nav_data)
        logger.info(f"Seeded {len(nav_data)} NAV records")

    except Exception as e:
        logger.error(f"Failed to seed NAV history table: {e}")
        raise


def seed_factor_data_table(client: DatabaseClient) -> None:
    """Seed factor data table with mock data."""
    try:
        # Read mock Fama-French data
        ff_file = "data/raw/fama_french_3factor.parquet"
        if not os.path.exists(ff_file):
            logger.warning(f"Fama-French file not found: {ff_file}")
            return

        df = pd.read_parquet(ff_file)

        # Convert to list of dictionaries
        factor_data = []
        for _, row in df.iterrows():
            factor_data.append(
                {
                    "date": row["date"].isoformat(),
                    "mkt_rf": float(row["mkt_rf"]),
                    "smb": float(row["smb"]),
                    "hml": float(row["hml"]),
                    "rf": float(row["rf"]),
                }
            )

        # Upsert factor data
        client.upsert_factor_data(factor_data)
        logger.info(f"Seeded {len(factor_data)} factor records")

    except Exception as e:
        logger.error(f"Failed to seed factor data table: {e}")
        raise


def seed_database() -> None:
    """Seed all database tables with mock data."""
    logger.info("Starting database seeding...")

    try:
        # Get database client
        client = get_db_client()

        # Seed tables in order
        seed_funds_table(client)
        seed_nav_history_table(client)
        seed_factor_data_table(client)

        logger.info("Database seeding completed successfully")

    except Exception as e:
        logger.error(f"Database seeding failed: {e}")
        raise


if __name__ == "__main__":
    seed_database()
