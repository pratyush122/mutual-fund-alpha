"""
Upsert Alpha Results to Supabase Module
"""

import pandas as pd
import numpy as np
import os
import json
from typing import List, Dict
from src.database.client import get_db_client
from src.utils.logger import logger


def prepare_alpha_results_for_upsert(
    bootstrap_results: pd.DataFrame,
    skill_metrics: pd.DataFrame,
    fund_classifications: pd.DataFrame,
) -> List[Dict]:
    """
    Prepare alpha results for upserting to Supabase.

    Args:
        bootstrap_results: DataFrame with bootstrap test results
        skill_metrics: DataFrame with skill metrics
        fund_classifications: DataFrame with fund classifications

    Returns:
        List of dictionaries ready for upsert
    """
    logger.info("Preparing alpha results for upsert...")

    try:
        # Merge all results
        # First, merge bootstrap results with skill metrics
        merged_df = pd.merge(
            bootstrap_results,
            skill_metrics[
                [
                    "scheme_code",
                    "sharpe_ratio",
                    "info_ratio",
                    "composite_skill_score",
                    "percentile_rank",
                ]
            ],
            on="scheme_code",
            how="left",
        )

        # Then merge with fund classifications
        merged_df = pd.merge(
            merged_df,
            fund_classifications[["scheme_code", "overall_verdict"]],
            on="scheme_code",
            how="left",
        )

        # Group by fund and aggregate for final results
        final_results = []

        for fund in merged_df["scheme_code"].unique():
            fund_data = merged_df[merged_df["scheme_code"] == fund]

            # Aggregate metrics
            avg_alpha = fund_data["alpha"].mean()
            avg_beta_mkt = fund_data["beta_mkt"].mean()
            avg_beta_smb = fund_data["beta_smb"].mean()
            avg_beta_hml = fund_data["beta_hml"].mean()
            avg_r_squared = fund_data["r_squared"].mean()
            avg_t_stat_alpha = fund_data["t_stat_alpha"].mean()
            avg_p_value = fund_data["bootstrap_p_value"].mean()
            sharpe = (
                fund_data["sharpe_ratio"].iloc[0]
                if not fund_data["sharpe_ratio"].isnull().all()
                else 0
            )
            info_ratio = (
                fund_data["info_ratio"].iloc[0]
                if not fund_data["info_ratio"].isnull().all()
                else 0
            )
            skill_score = (
                fund_data["composite_skill_score"].iloc[0]
                if not fund_data["composite_skill_score"].isnull().all()
                else 0
            )
            percentile_rank = (
                fund_data["percentile_rank"].iloc[0]
                if not fund_data["percentile_rank"].isnull().all()
                else 0
            )
            verdict = (
                fund_data["overall_verdict"].iloc[0]
                if not fund_data["overall_verdict"].isnull().all()
                else "Unknown"
            )

            # Prepare factor exposures as JSON
            factor_exposures = {
                "beta_mkt": avg_beta_mkt,
                "beta_smb": avg_beta_smb,
                "beta_hml": avg_beta_hml,
            }

            # Prepare rolling windows data as JSON
            rolling_windows = []
            for _, row in fund_data.iterrows():
                rolling_windows.append(
                    {
                        "start_date": (
                            row["start_date"].isoformat()
                            if hasattr(row["start_date"], "isoformat")
                            else str(row["start_date"])
                        ),
                        "end_date": (
                            row["end_date"].isoformat()
                            if hasattr(row["end_date"], "isoformat")
                            else str(row["end_date"])
                        ),
                        "alpha": row["alpha"],
                        "t_stat": row["t_stat_alpha"],
                        "p_value": row["bootstrap_p_value"],
                        "verdict": row["bootstrap_verdict"],
                    }
                )

            # Create result record
            result_record = {
                "scheme_code": fund,
                "window_days": 756,  # Standard 3-year window
                "alpha": avg_alpha,
                "beta_mkt": avg_beta_mkt,
                "beta_smb": avg_beta_smb,
                "beta_hml": avg_beta_hml,
                "r_squared": avg_r_squared,
                "t_stat_alpha": avg_t_stat_alpha,
                "p_value": avg_p_value,
                "sharpe": sharpe,
                "info_ratio": info_ratio,
                "skill_score": skill_score,
                "percentile_rank": percentile_rank,
                "alpha_persistence": 0.0,  # Would compute from autocorrelation in practice
                "factor_exposures": json.dumps(factor_exposures),
                "rolling_windows": json.dumps(rolling_windows),
            }

            final_results.append(result_record)

        logger.info(f"Prepared {len(final_results)} alpha results for upsert")
        return final_results

    except Exception as e:
        logger.error(f"Failed to prepare alpha results for upsert: {e}")
        raise


def upsert_alpha_results_to_db(alpha_results: List[Dict], batch_size: int = 20) -> None:
    """
    Upsert alpha results to Supabase database.

    Args:
        alpha_results: List of alpha result dictionaries
        batch_size: Number of records to upsert in each batch
    """
    logger.info(f"Upserting {len(alpha_results)} alpha results to database...")

    try:
        # Get database client
        db_client = get_db_client()

        # Upsert in batches
        for i in range(0, len(alpha_results), batch_size):
            batch = alpha_results[i : i + batch_size]
            db_client.upsert_alpha_results(batch, batch_size=len(batch))
            logger.info(f"Upserted batch of {len(batch)} alpha results")

        logger.info("Successfully upserted all alpha results to database")

    except Exception as e:
        logger.error(f"Failed to upsert alpha results to database: {e}")
        raise


def main() -> None:
    """Main function to prepare and upsert alpha results."""
    logger.info("Starting alpha results upsert process...")

    try:
        # Read required data files
        data_files = {
            "bootstrap_results": "data/processed/bootstrap_results.parquet",
            "skill_metrics": "data/processed/skill_metrics.parquet",
            "fund_classifications": "data/processed/fund_classifications.parquet",
        }

        dataframes = {}
        for name, filepath in data_files.items():
            if os.path.exists(filepath):
                dataframes[name] = pd.read_parquet(filepath)
                logger.info(f"Loaded {name}: {len(dataframes[name])} records")
            else:
                logger.error(f"File not found: {filepath}")
                return

        # Prepare alpha results for upsert
        alpha_results = prepare_alpha_results_for_upsert(
            bootstrap_results=dataframes["bootstrap_results"],
            skill_metrics=dataframes["skill_metrics"],
            fund_classifications=dataframes["fund_classifications"],
        )

        # Upsert to database
        upsert_alpha_results_to_db(alpha_results)

        logger.info("Alpha results upsert process completed successfully")

    except Exception as e:
        logger.error(f"Alpha results upsert process failed: {e}")
        raise


if __name__ == "__main__":
    main()
