"""
Test Bootstrap Significance Test Module
"""

import pandas as pd
import os
from typing import List, Dict
from src.analysis.bootstrap_test import perform_bootstrap_tests, classify_funds_by_skill
from src.utils.logger import logger


def test_bootstrap_significance() -> None:
    """Test the bootstrap significance test."""
    logger.info("Testing bootstrap significance test...")

    try:
        # Read regression results
        regression_file = "data/processed/regression_results.parquet"
        if not os.path.exists(regression_file):
            logger.error(f"Regression results file not found: {regression_file}")
            return

        regression_results = pd.read_parquet(regression_file)
        logger.info(f"Loaded regression results: {len(regression_results)} records")

        # Read aligned data
        aligned_file = "data/processed/aligned_data.parquet"
        if not os.path.exists(aligned_file):
            logger.error(f"Aligned data file not found: {aligned_file}")
            return

        aligned_data = pd.read_parquet(aligned_file)
        logger.info(f"Loaded aligned data: {len(aligned_data)} records")

        # Perform bootstrap tests (reduced number for faster testing)
        bootstrap_results = perform_bootstrap_tests(
            regression_results=regression_results,
            aligned_data=aligned_data,
            n_bootstrap=100,  # Reduced for faster testing
        )

        # Print sample results
        if not bootstrap_results.empty:
            print("\nSample bootstrap test results:")
            print(
                bootstrap_results[
                    ["scheme_code", "alpha", "bootstrap_p_value", "bootstrap_verdict"]
                ].head(10)
            )

            # Save results
            output_file = "data/processed/bootstrap_results.parquet"
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            bootstrap_results.to_parquet(output_file, index=False)
            logger.info(f"Saved bootstrap results to {output_file}")

            # Classify funds by skill
            fund_classifications = classify_funds_by_skill(bootstrap_results)

            # Print sample classifications
            print("\nSample fund classifications:")
            print(fund_classifications.head(10))

            # Save classifications
            classification_file = "data/processed/fund_classifications.parquet"
            fund_classifications.to_parquet(classification_file, index=False)
            logger.info(f"Saved fund classifications to {classification_file}")

            # Show summary
            print(f"\nFund classification summary:")
            verdict_counts = fund_classifications["overall_verdict"].value_counts()
            for verdict, count in verdict_counts.items():
                print(f"  {verdict}: {count} funds")

    except Exception as e:
        logger.error(f"Bootstrap significance test failed: {e}")
        raise


if __name__ == "__main__":
    test_bootstrap_significance()
