"""
Real Fama-French Factor Data Ingestion Module
"""

import requests
import pandas as pd
import zipfile
import os
from io import BytesIO
from typing import Optional
import logging
from src.utils.retry import retry
from src.utils.cache import Cache
from src.utils.logger import logger

# Initialize cache with 24-hour TTL
cache = Cache(ttl_hours=24)

@retry(n=3, backoff=2)
def fetch_fama_french_factors() -> Optional[pd.DataFrame]:
    """
    Fetch Fama-French 3-factor data from Kenneth French's data library.

    Returns:
        DataFrame with date, mkt_rf, smb, hml, rf columns
    """
    # Check cache first
    cached_data = cache.get("fama_french_factors")
    if cached_data:
        logger.info("Using cached Fama-French factors")
        # Convert cached data back to DataFrame
        return pd.DataFrame(cached_data)

    # Primary source: Ken French data
    url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_daily_CSV.zip"

    try:
        logger.info("Fetching Fama-French factors from primary source...")
        # Download ZIP file
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # Extract CSV from ZIP
        with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
            # Find the CSV file (usually named something like F-F_Research_Data_Factors_daily.CSV)
            csv_filename = None
            for filename in zip_file.namelist():
                if filename.endswith('.CSV') and 'Factors' in filename:
                    csv_filename = filename
                    break

            if not csv_filename:
                # Try any CSV file if specific naming not found
                for filename in zip_file.namelist():
                    if filename.endswith('.CSV'):
                        csv_filename = filename
                        break

            if not csv_filename:
                raise ValueError("No CSV file found in ZIP archive")

            logger.info(f"Found CSV file: {csv_filename}")

            # Read CSV content
            with zip_file.open(csv_filename) as csv_file:
                # Read content and decode
                content = csv_file.read().decode('latin1')
                lines = content.split('\n')

                # Find where data starts (look for first line with numeric data)
                data_start = 0
                headers_line = 0
                for i, line in enumerate(lines):
                    if line.strip() and ('Mkt-RF' in line or 'SMB' in line or 'HML' in line):
                        headers_line = i
                        continue
                    if line.strip() and line[0].isdigit():
                        data_start = i
                        break

                # Extract headers
                if headers_line < data_start:
                    headers = lines[headers_line].strip().split(',')
                    # Clean headers
                    headers = [h.strip().replace('"', '') for h in headers]
                else:
                    headers = ['date', 'mkt_rf', 'smb', 'hml', 'rf']

                # Extract data lines
                data_lines = lines[data_start:]

                # Parse data lines
                data_rows = []
                for line in data_lines:
                    if line.strip() and line[0].isdigit():
                        values = line.strip().split(',')
                        if len(values) >= 5:  # Ensure we have enough columns
                            try:
                                # Convert date and factor values
                                date = pd.to_datetime(values[0].strip(), format='%Y%m%d')

                                # Convert factor values (they're multiplied by 100 in the data)
                                mkt_rf = float(values[1]) / 100 if values[1].strip() != '' else 0
                                smb = float(values[2]) / 100 if values[2].strip() != '' else 0
                                hml = float(values[3]) / 100 if values[3].strip() != '' else 0
                                rf = float(values[4]) / 100 if values[4].strip() != '' else 0

                                data_rows.append([date, mkt_rf, smb, hml, rf])
                            except (ValueError, IndexError):
                                # Skip invalid rows
                                continue

                # Create DataFrame
                if data_rows:
                    df = pd.DataFrame(data_rows, columns=['date', 'mkt_rf', 'smb', 'hml', 'rf'])

                    # Remove any duplicates and sort by date
                    df = df.drop_duplicates(subset=['date']).sort_values('date').reset_index(drop=True)

                    # Cache the data
                    cache.set("fama_french_factors", df.to_dict('records'))

                    logger.info(f"Successfully fetched Fama-French factors: {len(df)} records")
                    return df
                else:
                    raise ValueError("No valid data found in CSV")

    except Exception as e:
        logger.error(f"Failed to fetch Fama-French factors from primary source: {e}")

        # Fallback: Try to get factors from FRED API if available
        logger.info("Attempting to fetch factors from FRED API as fallback")
        return fetch_fred_factors()

def fetch_fred_factors() -> Optional[pd.DataFrame]:
    """
    Fetch factor data from FRED API as fallback.
    This is a placeholder implementation - would need actual FRED API key and implementation.

    Returns:
        DataFrame with date, mkt_rf, smb, hml, rf columns or None
    """
    try:
        # This would require FRED API key and actual implementation
        # For now, we'll return None to indicate failure
        logger.warning("FRED API fallback not implemented yet - returning None")
        return None
    except Exception as e:
        logger.error(f"Error in FRED fallback: {e}")
        return None

def save_fama_french_data(df: pd.DataFrame, output_dir: str = "data/raw/") -> str:
    """
    Save Fama-French factor data to parquet.

    Args:
        df: DataFrame with factor data
        output_dir: Directory to save the parquet file

    Returns:
        Path to saved file
    """
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "fama_french_3factor.parquet")
    df.to_parquet(output_file, index=False)
    logger.info(f"Saved Fama-French factors to {output_file}")
    return output_file

if __name__ == "__main__":
    # Example usage
    df = fetch_fama_french_factors()
    if df is not None:
        print(f"Fetched {len(df)} records of Fama-French factors")
        print(df.head())
        save_fama_french_data(df)
    else:
        print("Failed to fetch Fama-French factors")