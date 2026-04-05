"""
Factor Alignment Module
"""

import pandas as pd
import os
from src.utils.logger import logger


def align_factors_to_returns(
    returns_df: pd.DataFrame, factors_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Align Fama-French factors to same date index as fund returns.

    Args:
        returns_df: DataFrame with fund returns
        factors_df: DataFrame with Fama-French factors

    Returns:
        DataFrame with aligned factors
    """
    logger.info("Aligning Fama-French factors to fund returns...")

    try:
        # Convert date columns to datetime
        returns_df["date"] = pd.to_datetime(returns_df["date"])
        factors_df["date"] = pd.to_datetime(factors_df["date"])

        # Get unique dates from returns data
        return_dates = returns_df["date"].unique()
        logger.info(f"Fund returns cover {len(return_dates)} unique dates")

        # Filter factors to only include dates that exist in returns
        aligned_factors = factors_df[factors_df["date"].isin(return_dates)].copy()
        logger.info(f"Aligned factors to {len(aligned_factors)} dates")

        return aligned_factors

    except Exception as e:
        logger.error(f"Failed to align factors: {e}")
        raise


def compute_excess_returns(
    returns_df: pd.DataFrame, factors_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Compute excess returns: fund_return - rf (risk-free rate).

    Args:
        returns_df: DataFrame with fund returns
        factors_df: DataFrame with Fama-French factors (must be aligned)

    Returns:
        DataFrame with excess returns added
    """
    logger.info("Computing excess returns...")

    try:
        # Convert date columns to datetime
        returns_df["date"] = pd.to_datetime(returns_df["date"])
        factors_df["date"] = pd.to_datetime(factors_df["date"])

        # Merge returns with risk-free rate
        merged_df = pd.merge(
            returns_df, factors_df[["date", "rf"]], on="date", how="left"
        )

        # Compute excess returns
        merged_df["excess_return"] = merged_df["daily_return"] - merged_df["rf"]

        logger.info(
            f"Computed excess returns for {merged_df['scheme_code'].nunique()} funds"
        )
        return merged_df

    except Exception as e:
        logger.error(f"Failed to compute excess returns: {e}")
        raise


def save_aligned_data(df: pd.DataFrame, output_dir: str = "data/processed/") -> str:
    """
    Save aligned data to parquet.

    Args:
        df: DataFrame with aligned data
        output_dir: Directory to save the file

    Returns:
        Path to saved file
    """
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "aligned_data.parquet")
    df.to_parquet(output_file, index=False)
    logger.info(f"Saved aligned data to {output_file}")
    return output_file


def main():
    """Main function to align factors and compute excess returns."""
    logger.info("Starting factor alignment pipeline...")

    try:
        # Read processed returns data
        returns_file = "data/processed/fund_returns.parquet"
        if not os.path.exists(returns_file):
            logger.error(f"Returns file not found: {returns_file}")
            return

        returns_df = pd.read_parquet(returns_file)
        logger.info(
            f"Loaded returns data: {len(returns_df)} records for {returns_df['scheme_code'].nunique()} funds"
        )

        # Read Fama-French factor data
        factors_file = "data/raw/fama_french_3factor.parquet"
        if not os.path.exists(factors_file):
            logger.error(f"Factors file not found: {factors_file}")
            return

        factors_df = pd.read_parquet(factors_file)
        logger.info(f"Loaded factor data: {len(factors_df)} records")

        # Align factors to returns
        aligned_factors = align_factors_to_returns(returns_df, factors_df)

        # Compute excess returns
        df_with_excess = compute_excess_returns(returns_df, aligned_factors)

        # Save aligned data
        save_aligned_data(df_with_excess)
        logger.info("Factor alignment pipeline completed successfully")

        return df_with_excess

    except Exception as e:
        logger.error(f"Factor alignment pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()
