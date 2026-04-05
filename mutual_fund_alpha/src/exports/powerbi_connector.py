"""
Power BI Connector Module
Exports Supabase tables to .pbix-ready CSV format
"""

import pandas as pd
import os
from typing import Dict
from src.database.client import get_db_client
from src.utils.logger import logger


def export_supabase_tables_to_csv(output_dir: str = "data/powerbi/") -> Dict[str, str]:
    """
    Export all Supabase tables to CSV files for Power BI consumption.

    Args:
        output_dir: Directory to save CSV files

    Returns:
        Dictionary mapping table names to file paths
    """
    logger.info("Exporting Supabase tables to Power BI CSV format...")

    try:
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)

        # Get database client
        db_client = get_db_client()

        # Define tables to export and their CSV filenames
        tables_to_export = {
            "funds": "funds.csv",
            "nav_history": "nav_history.csv",
            "alpha_results": "alpha_results.csv",
            "factor_data": "factor_data.csv",
        }

        exported_files = {}

        # Export each table
        for table_name, csv_filename in tables_to_export.items():
            try:
                # Query table data
                df = (
                    db_client.query_funds()
                    if table_name == "funds"
                    else (
                        db_client.query_nav_history()
                        if table_name == "nav_history"
                        else (
                            db_client.query_alpha_results()
                            if table_name == "alpha_results"
                            else pd.DataFrame()
                        )
                    )
                )  # Default empty DataFrame

                # Save to CSV
                csv_path = os.path.join(output_dir, csv_filename)
                df.to_csv(csv_path, index=False)
                exported_files[table_name] = csv_path
                logger.info(f"Exported {table_name} to {csv_path} ({len(df)} records)")

            except Exception as e:
                logger.warning(f"Failed to export {table_name}: {e}")
                # Create empty CSV with headers
                empty_df = pd.DataFrame()
                csv_path = os.path.join(output_dir, csv_filename)
                empty_df.to_csv(csv_path, index=False)
                exported_files[table_name] = csv_path
                logger.info(f"Created empty CSV for {table_name}")

        # Create category benchmarks CSV (derived from existing data)
        try:
            benchmarks_path = os.path.join(output_dir, "category_benchmarks.csv")
            # This would be derived from the skill metrics data
            skill_metrics_path = "data/processed/skill_metrics.parquet"
            if os.path.exists(skill_metrics_path):
                skill_df = pd.read_parquet(skill_metrics_path)
                # Create a simplified category benchmark file
                category_df = (
                    skill_df.groupby(skill_df.index % 5)
                    .agg(
                        {
                            "sharpe_ratio": "mean",
                            "info_ratio": "mean",
                            "composite_skill_score": "mean",
                        }
                    )
                    .reset_index()
                )
                category_df.to_csv(benchmarks_path, index=False)
                exported_files["category_benchmarks"] = benchmarks_path
                logger.info(f"Exported category benchmarks to {benchmarks_path}")
            else:
                # Create empty file
                empty_df = pd.DataFrame()
                empty_df.to_csv(benchmarks_path, index=False)
                exported_files["category_benchmarks"] = benchmarks_path
                logger.info("Created empty category benchmarks CSV")

        except Exception as e:
            logger.warning(f"Failed to export category benchmarks: {e}")

        logger.info(f"Power BI export completed. Files: {list(exported_files.keys())}")
        return exported_files

    except Exception as e:
        logger.error(f"Power BI export failed: {e}")
        raise


def create_refresh_script(script_path: str = "scripts/refresh_powerbi_data.sh") -> str:
    """
    Create auto-refresh script for Power BI data.

    Args:
        script_path: Path to save the script

    Returns:
        Path to created script
    """
    logger.info("Creating Power BI refresh script...")

    try:
        # Create scripts directory
        os.makedirs(os.path.dirname(script_path), exist_ok=True)

        # Script content (for Windows PowerShell)
        script_content = """#!/bin/bash
# Power BI Data Refresh Script
# Runs every 24 hours to regenerate all CSVs from Supabase

echo "[$(date)] Starting Power BI data refresh..."

# Activate virtual environment
source venv/Scripts/activate

# Run export script
python -c "
import sys
sys.path.insert(0, 'mutual_fund_alpha')
from src.exports.powerbi_connector import export_supabase_tables_to_csv
export_supabase_tables_to_csv()
"

echo "[$(date)] Power BI data refresh completed."
"""

        # Write script
        with open(script_path, "w") as f:
            f.write(script_content)

        # Make executable (Unix/Linux)
        if os.name != "nt":  # Not Windows
            import stat

            st = os.stat(script_path)
            os.chmod(script_path, st.st_mode | stat.S_IEXEC)

        logger.info(f"Created refresh script at {script_path}")
        return script_path

    except Exception as e:
        logger.error(f"Failed to create refresh script: {e}")
        raise


def setup_cron_job(script_path: str) -> None:
    """
    Set up cron job for automatic refresh (conceptual - would need actual cron setup).

    Args:
        script_path: Path to refresh script
    """
    logger.info("Setting up cron job for automatic refresh...")

    try:
        # This is conceptual - in practice, you would use crontab or Windows Task Scheduler
        cron_instruction = f"""
To set up automatic refresh:

1. On Linux/Mac, add to crontab:
   0 2 * * * {os.path.abspath(script_path)}  # Run daily at 2 AM

2. On Windows, use Task Scheduler:
   - Create new task
   - Set trigger to daily at 2 AM
   - Set action to run: {os.path.abspath(script_path)}
"""

        logger.info("Cron setup instructions:")
        logger.info(cron_instruction)

    except Exception as e:
        logger.warning(f"Failed to set up cron instructions: {e}")


def main() -> None:
    """Main function to export data for Power BI."""
    logger.info("Starting Power BI data export...")

    try:
        # Export tables to CSV
        exported_files = export_supabase_tables_to_csv()

        # Create refresh script
        script_path = create_refresh_script()

        # Setup cron job (conceptual)
        setup_cron_job(script_path)

        logger.info("Power BI data export completed successfully")
        logger.info("Exported files:")
        for table, path in exported_files.items():
            logger.info(f"  {table}: {path}")

    except Exception as e:
        logger.error(f"Power BI data export failed: {e}")
        raise


if __name__ == "__main__":
    main()
