"""
Benchmark Data Ingestion Module
"""

import pandas as pd
import yfinance as yf
import os
from typing import Optional
from src.utils.retry import retry
from src.utils.cache import Cache
from src.utils.logger import logger

# Initialize cache with 24-hour TTL
cache = Cache(ttl_hours=24)


@retry(n=3, backoff=2)
def fetch_benchmark_data(ticker: str, period: str = "max") -> Optional[pd.DataFrame]:
    """
    Fetch benchmark data from Yahoo Finance.

    Args:
        ticker: Yahoo Finance ticker symbol
        period: Data period ("1y", "2y", "5y", "10y", "ytd", "max")

    Returns:
        DataFrame with benchmark data or None if failed
    """
    # Check cache first
    cache_key = f"benchmark_{ticker}_{period}"
    cached_data = cache.get(cache_key)
    if cached_data:
        logger.info(f"Using cached benchmark data for {ticker}")
        return pd.DataFrame(cached_data)

    try:
        logger.info(f"Fetching benchmark data for {ticker}...")
        # Fetch data from Yahoo Finance
        data = yf.download(ticker, period=period)

        if not data.empty:
            # Reset index to make Date a column
            data = data.reset_index()

            # Rename columns to lowercase
            data.columns = [col.lower() for col in data.columns]

            # Cache the data
            cache.set(cache_key, data.to_dict("records"))

            logger.info(
                f"Successfully fetched benchmark data for {ticker}: {len(data)} records"
            )
            return data
        else:
            logger.warning(f"No data returned for benchmark {ticker}")
            return None

    except Exception as e:
        logger.error(f"Failed to fetch benchmark data for {ticker}: {e}")
        return None


def fetch_multiple_benchmarks(tickers: dict, period: str = "max") -> pd.DataFrame:
    """
    Fetch multiple benchmark indices.

    Args:
        tickers: Dictionary mapping ticker symbols to names
        period: Data period

    Returns:
        Combined DataFrame with all benchmark data
    """
    all_data = []

    for ticker, name in tickers.items():
        try:
            data = fetch_benchmark_data(ticker, period)
            if data is not None:
                # Add ticker info
                data["ticker"] = ticker
                data["name"] = name
                all_data.append(data)
        except Exception as e:
            logger.error(f"Failed to process benchmark {ticker}: {e}")

    if all_data:
        # Combine all data
        combined_df = pd.concat(all_data, ignore_index=True)
        return combined_df
    else:
        logger.error("No benchmark data fetched successfully")
        return pd.DataFrame()


def save_benchmark_data(df: pd.DataFrame, output_dir: str = "data/raw/") -> str:
    """
    Save benchmark data to parquet.

    Args:
        df: DataFrame with benchmark data
        output_dir: Directory to save the parquet file

    Returns:
        Path to saved file
    """
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "benchmarks.parquet")
    df.to_parquet(output_file, index=False)
    logger.info(f"Saved benchmark data to {output_file}")
    return output_file


def main():
    """Main function to fetch and save benchmark data."""
    # Define benchmarks to fetch
    benchmarks = {"^NSEI": "Nifty 50", "^NSMDCP50": "Nifty 500"}

    # Fetch benchmark data
    df = fetch_multiple_benchmarks(benchmarks, period="max")

    if not df.empty:
        # Save to file
        output_file = save_benchmark_data(df)
        print(f"Saved benchmark data to {output_file}")
        print(f"Data shape: {df.shape}")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
        return df
    else:
        print("Failed to fetch benchmark data")
        return None


if __name__ == "__main__":
    main()
