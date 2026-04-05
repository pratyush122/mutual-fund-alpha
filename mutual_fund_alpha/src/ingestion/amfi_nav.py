"""
AMFI NAV Data Ingestion Module
"""

import requests
import pandas as pd
import json
import os
from typing import Dict, List, Optional
from datetime import datetime
import logging
from src.utils.retry import retry
from src.utils.cache import Cache
from src.utils.logger import logger

# Initialize cache with 24-hour TTL
cache = Cache(ttl_hours=24)

@retry(n=3, backoff=2)
def fetch_all_scheme_codes() -> List[Dict]:
    """
    Fetch all mutual fund scheme codes from AMFI API.

    Returns:
        List of scheme dictionaries with code and name
    """
    url = "https://api.mfapi.in/mf"

    # Check cache first
    cached_data = cache.get("amfi_all_schemes")
    if cached_data:
        logger.info("Using cached scheme codes")
        return cached_data

    try:
        response = requests.get(url)
        response.raise_for_status()
        schemes = response.json()

        # Cache the response
        cache.set("amfi_all_schemes", schemes)

        logger.info(f"Fetched {len(schemes)} scheme codes from AMFI")
        return schemes
    except Exception as e:
        logger.error(f"Failed to fetch scheme codes: {e}")
        raise

@retry(n=3, backoff=2)
def fetch_scheme_nav_history(scheme_code: str) -> Optional[pd.DataFrame]:
    """
    Fetch NAV history for a specific scheme.

    Args:
        scheme_code: Scheme code to fetch

    Returns:
        DataFrame with date and nav columns, or None if failed
    """
    url = f"https://api.mfapi.in/mf/{scheme_code}"

    # Check cache first
    cached_data = cache.get(f"amfi_nav_{scheme_code}")
    if cached_data:
        logger.info(f"Using cached NAV data for scheme {scheme_code}")
        return pd.DataFrame(cached_data)

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Extract NAV history
        nav_history = []
        for item in data.get('data', []):
            try:
                nav_history.append({
                    'date': pd.to_datetime(item['date']),
                    'nav': float(item['nav'])
                })
            except (KeyError, ValueError):
                # Skip invalid entries
                continue

        # Convert to DataFrame
        df = pd.DataFrame(nav_history)
        if not df.empty:
            df = df.sort_values('date').reset_index(drop=True)

            # Cache the response
            cache.set(f"amfi_nav_{scheme_code}", nav_history)

            logger.info(f"Fetched NAV history for scheme {scheme_code}: {len(df)} records")
            return df
        else:
            logger.warning(f"No valid NAV data for scheme {scheme_code}")
            return None

    except Exception as e:
        logger.error(f"Failed to fetch NAV history for scheme {scheme_code}: {e}")
        return None

def fetch_top_n_equity_funds_by_aum(n: int = 200) -> List[Dict]:
    """
    Fetch top N equity funds by AUM.

    Args:
        n: Number of top funds to fetch

    Returns:
        List of top N equity fund schemes
    """
    try:
        all_schemes = fetch_all_scheme_codes()

        # Filter for equity funds and sort by AUM
        # Note: This is a simplified approach - in reality, you'd need to fetch AUM data separately
        equity_schemes = [scheme for scheme in all_schemes
                         if 'equity' in scheme.get('schemeName', '').lower()]

        # Return top N (this is a placeholder - real implementation would sort by actual AUM)
        top_schemes = equity_schemes[:n]
        logger.info(f"Selected top {len(top_schemes)} equity funds")
        return top_schemes

    except Exception as e:
        logger.error(f"Failed to fetch top equity funds: {e}")
        return []

def fetch_all_nav_data(output_dir: str = "data/raw/") -> pd.DataFrame:
    """
    Fetch NAV data for top funds and save to parquet.

    Args:
        output_dir: Directory to save the parquet file

    Returns:
        Combined DataFrame with all fund NAV data
    """
    os.makedirs(output_dir, exist_ok=True)

    # Get top funds
    top_funds = fetch_top_n_equity_funds_by_aum()

    # Collect all NAV data
    all_data = []
    failed_schemes = []

    for scheme in top_funds:
        scheme_code = scheme.get('schemeCode')
        scheme_name = scheme.get('schemeName')

        if not scheme_code:
            continue

        try:
            nav_df = fetch_scheme_nav_history(scheme_code)
            if nav_df is not None and not nav_df.empty:
                # Add scheme info
                nav_df['scheme_code'] = scheme_code
                nav_df['scheme_name'] = scheme_name
                all_data.append(nav_df)
            else:
                failed_schemes.append(scheme_code)
        except Exception as e:
            logger.error(f"Failed to process scheme {scheme_code}: {e}")
            failed_schemes.append(scheme_code)

    if all_data:
        # Combine all data
        combined_df = pd.concat(all_data, ignore_index=True)

        # Reorder columns
        combined_df = combined_df[['scheme_code', 'scheme_name', 'date', 'nav']]

        # Save to parquet
        output_file = os.path.join(output_dir, "amfi_nav.parquet")
        combined_df.to_parquet(output_file, index=False)
        logger.info(f"Saved combined NAV data to {output_file}")

        # Log failures
        if failed_schemes:
            logger.warning(f"Failed to fetch data for {len(failed_schemes)} schemes: {failed_schemes}")

        return combined_df
    else:
        logger.error("No NAV data fetched successfully")
        return pd.DataFrame()

if __name__ == "__main__":
    # Example usage
    df = fetch_all_nav_data()
    print(f"Fetched {len(df)} records from {df['scheme_code'].nunique()} funds")