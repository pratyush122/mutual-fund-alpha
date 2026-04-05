"""
Test Factor Model Module
"""

import pandas as pd
import os
from src.analysis.factor_model import run_rolling_regressions
from src.utils.logger import logger


def test_factor_model() -> None:
    """Test the factor model with processed data."""
    logger.info("Testing factor model...")

    try:
        # Read aligned data
        aligned_file = "data/processed/aligned_data.parquet"
        if not os.path.exists(aligned_file):
            logger.error(f"Aligned data file not found: {aligned_file}")
            return

        aligned_df = pd.read_parquet(aligned_file)
        logger.info(
            f"Loaded aligned data: {len(aligned_df)} records for {aligned_df['scheme_code'].nunique()} funds"
        )

        # Read factor data
        factors_file = "data/raw/fama_french_3factor.parquet"
        if not os.path.exists(factors_file):
            logger.error(f"Factors file not found: {factors_file}")
            return

        factors_df = pd.read_parquet(factors_file)
        logger.info(f"Loaded factor data: {len(factors_df)} records")

        # Run rolling regressions (smaller window for testing)
        results = run_rolling_regressions(
            excess_returns=aligned_df,
            factors=factors_df,
            window=63,  # 3 months for testing
            step=21,  # 1 month step
        )

        logger.info(f"Completed {len(results)} rolling regressions")

        # Print sample results
        if results:
            print("\nSample regression results:")
            for i, result in enumerate(results[:5]):  # Show first 5
                print(
                    f"  {i+1}. Fund {result['scheme_code']}: α={result['alpha']:.6f}, t-stat={result['t_stat_alpha']:.2f}, R²={result['r_squared']:.3f}"
                )

        # Save results
        if results:
            results_df = pd.DataFrame(results)
            output_file = "data/processed/regression_results.parquet"
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            results_df.to_parquet(output_file, index=False)
            logger.info(f"Saved regression results to {output_file}")

    except Exception as e:
        logger.error(f"Factor model test failed: {e}")
        raise


if __name__ == "__main__":
    test_factor_model()
