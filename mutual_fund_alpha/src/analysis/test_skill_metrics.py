"""
Test Skill Metrics Module
"""

import pandas as pd
import os
from src.analysis.skill_metrics import compute_all_skill_metrics
from src.utils.logger import logger


def test_skill_metrics() -> None:
    """Test the skill metrics computation."""
    logger.info("Testing skill metrics...")

    try:
        # Read fund returns data
        returns_file = "data/processed/fund_returns.parquet"
        if not os.path.exists(returns_file):
            logger.error(f"Returns file not found: {returns_file}")
            return

        fund_data = pd.read_parquet(returns_file)
        logger.info(
            f"Loaded fund returns: {len(fund_data)} records for {fund_data['scheme_code'].nunique()} funds"
        )

        # Read benchmark data
        benchmark_file = "data/raw/benchmarks.parquet"
        if not os.path.exists(benchmark_file):
            logger.error(f"Benchmark file not found: {benchmark_file}")
            return

        benchmark_data = pd.read_parquet(benchmark_file)
        logger.info(f"Loaded benchmark data: {len(benchmark_data)} records")

        # Read regression results
        regression_file = "data/processed/regression_results.parquet"
        if not os.path.exists(regression_file):
            logger.error(f"Regression results file not found: {regression_file}")
            return

        regression_results = pd.read_parquet(regression_file)
        logger.info(f"Loaded regression results: {len(regression_results)} records")

        # Compute skill metrics
        skill_metrics = compute_all_skill_metrics(
            fund_data=fund_data,
            benchmark_data=benchmark_data,
            regression_results=regression_results,
        )

        # Print sample results
        if not skill_metrics.empty:
            print("\nSample skill metrics:")
            print(
                skill_metrics[
                    [
                        "scheme_code",
                        "sharpe_ratio",
                        "info_ratio",
                        "composite_skill_score",
                        "percentile_rank",
                    ]
                ].head()
            )

            # Save results
            output_file = "data/processed/skill_metrics.parquet"
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            skill_metrics.to_parquet(output_file, index=False)
            logger.info(f"Saved skill metrics to {output_file}")

            # Show summary statistics
            print("\nSkill metrics summary:")
            print(f"  Average Sharpe ratio: {skill_metrics['sharpe_ratio'].mean():.3f}")
            print(
                f"  Average Information ratio: {skill_metrics['info_ratio'].mean():.3f}"
            )
            print(
                f"  Average composite skill score: {skill_metrics['composite_skill_score'].mean():.1f}"
            )
            print("  Top 5 funds by skill score:")
            top_funds = skill_metrics.nlargest(5, "composite_skill_score")
            for _, fund in top_funds.iterrows():
                print(f"    {fund['scheme_code']}: {fund['composite_skill_score']:.1f}")

    except Exception as e:
        logger.error(f"Skill metrics test failed: {e}")
        raise


if __name__ == "__main__":
    test_skill_metrics()
