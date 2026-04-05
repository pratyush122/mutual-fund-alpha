"""
Supabase Database Client Module
"""

import os
from typing import Any, Dict, List, Optional
import pandas as pd
import json
from supabase import create_client, Client
from src.utils.logger import logger
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class DatabaseClient:
    """Supabase database client with helper methods."""

    def __init__(self):
        """Initialize Supabase client."""
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_KEY")

        if not self.url or not self.key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment variables")

        self.client: Client = create_client(self.url, self.key)
        logger.info("Supabase client initialized")

    def upsert_funds(self, funds_data: List[Dict[str, Any]]) -> None:
        """
        Upsert funds data.

        Args:
            funds_data: List of fund dictionaries
        """
        try:
            response = self.client.table("funds").upsert(funds_data).execute()
            logger.info(f"Upserted {len(funds_data)} funds")
        except Exception as e:
            logger.error(f"Failed to upsert funds: {e}")
            raise

    def upsert_nav_history(self, nav_data: List[Dict[str, Any]]) -> None:
        """
        Upsert NAV history data.

        Args:
            nav_data: List of NAV history dictionaries
        """
        try:
            # Process in batches to avoid timeouts
            batch_size = 1000
            for i in range(0, len(nav_data), batch_size):
                batch = nav_data[i:i + batch_size]
                response = self.client.table("nav_history").upsert(batch).execute()
                logger.info(f"Upserted batch of {len(batch)} NAV records")
        except Exception as e:
            logger.error(f"Failed to upsert NAV history: {e}")
            raise

    def upsert_factor_data(self, factor_data: List[Dict[str, Any]]) -> None:
        """
        Upsert factor data.

        Args:
            factor_data: List of factor data dictionaries
        """
        try:
            response = self.client.table("factor_data").upsert(factor_data).execute()
            logger.info(f"Upserted {len(factor_data)} factor records")
        except Exception as e:
            logger.error(f"Failed to upsert factor data: {e}")
            raise

    def upsert_alpha_results(self, alpha_data: List[Dict[str, Any]], batch_size: int = 20) -> None:
        """
        Upsert alpha results data in batches.

        Args:
            alpha_data: List of alpha results dictionaries
            batch_size: Number of records per batch
        """
        try:
            # Process in batches
            for i in range(0, len(alpha_data), batch_size):
                batch = alpha_data[i:i + batch_size]
                # Convert datetime objects to ISO strings
                for record in batch:
                    if 'computed_at' in record and hasattr(record['computed_at'], 'isoformat'):
                        record['computed_at'] = record['computed_at'].isoformat()
                    # Convert JSON fields to strings if needed
                    for key in ['factor_exposures', 'rolling_windows']:
                        if key in record and isinstance(record[key], (dict, list)):
                            record[key] = json.dumps(record[key])

                response = self.client.table("alpha_results").upsert(batch).execute()
                logger.info(f"Upserted batch of {len(batch)} alpha results")
        except Exception as e:
            logger.error(f"Failed to upsert alpha results: {e}")
            raise

    def query_funds(self, filters: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Query funds data.

        Args:
            filters: Optional filters to apply

        Returns:
            DataFrame with funds data
        """
        try:
            query = self.client.table("funds").select("*")

            if filters:
                for key, value in filters.items():
                    query = query.eq(key, value)

            response = query.execute()
            df = pd.DataFrame(response.data)
            logger.info(f"Queried {len(df)} funds")
            return df
        except Exception as e:
            logger.error(f"Failed to query funds: {e}")
            raise

    def query_nav_history(self, scheme_code: Optional[str] = None) -> pd.DataFrame:
        """
        Query NAV history data.

        Args:
            scheme_code: Optional scheme code to filter by

        Returns:
            DataFrame with NAV history data
        """
        try:
            query = self.client.table("nav_history").select("*")

            if scheme_code:
                query = query.eq("scheme_code", scheme_code)

            response = query.execute()
            df = pd.DataFrame(response.data)
            logger.info(f"Queried {len(df)} NAV records")
            return df
        except Exception as e:
            logger.error(f"Failed to query NAV history: {e}")
            raise

    def query_alpha_results(self, scheme_code: Optional[str] = None) -> pd.DataFrame:
        """
        Query alpha results data.

        Args:
            scheme_code: Optional scheme code to filter by

        Returns:
            DataFrame with alpha results data
        """
        try:
            query = self.client.table("alpha_results").select("*")

            if scheme_code:
                query = query.eq("scheme_code", scheme_code)

            response = query.execute()
            df = pd.DataFrame(response.data)
            logger.info(f"Queried {len(df)} alpha results")
            return df
        except Exception as e:
            logger.error(f"Failed to query alpha results: {e}")
            raise

    def bulk_insert(self, table_name: str, data: List[Dict[str, Any]]) -> None:
        """
        Bulk insert data into a table.

        Args:
            table_name: Name of the table
            data: List of dictionaries to insert
        """
        try:
            # Process in smaller batches to avoid timeouts
            batch_size = 500
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                response = self.client.table(table_name).insert(batch).execute()
                logger.info(f"Inserted batch of {len(batch)} records into {table_name}")
        except Exception as e:
            logger.error(f"Failed to bulk insert into {table_name}: {e}")
            raise

# Global database client instance
db_client: Optional[DatabaseClient] = None

def get_db_client() -> DatabaseClient:
    """Get singleton database client instance."""
    global db_client
    if db_client is None:
        db_client = DatabaseClient()
    return db_client

if __name__ == "__main__":
    # Example usage
    try:
        client = get_db_client()
        print("Database client initialized successfully")

        # Test query
        funds_df = client.query_funds()
        print(f"Found {len(funds_df)} funds in database")

    except Exception as e:
        print(f"Error: {e}")