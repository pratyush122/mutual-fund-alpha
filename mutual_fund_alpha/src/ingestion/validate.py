"""
Data Validation Module
"""

import pandas as pd
import os
import json
from typing import Dict, List
import logging
from src.utils.logger import logger


def validate_amfi_nav_data(df: pd.DataFrame) -> Dict[str, any]:
    """
    Validate AMFI NAV data.

    Args:
        df: DataFrame with AMFI NAV data

    Returns:
        Dictionary with validation results
    """
    logger.info("Validating AMFI NAV data...")
    results = {
        "total_records": len(df),
        "unique_funds": (
            df["scheme_code"].nunique() if "scheme_code" in df.columns else 0
        ),
        "date_range": {
            "start": str(df["date"].min()) if "date" in df.columns else None,
            "end": str(df["date"].max()) if "date" in df.columns else None,
        },
        "issues": [],
    }

    # Check for required columns
    required_columns = ["scheme_code", "scheme_name", "date", "nav"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        results["issues"].append(f"Missing columns: {missing_columns}")

    # Check for null values in critical columns
    if "scheme_code" in df.columns:
        null_scheme_codes = df["scheme_code"].isnull().sum()
        if null_scheme_codes > 0:
            results["issues"].append(f"Null scheme codes: {null_scheme_codes}")

    if "date" in df.columns:
        null_dates = df["date"].isnull().sum()
        if null_dates > 0:
            results["issues"].append(f"Null dates: {null_dates}")

    if "nav" in df.columns:
        null_navs = df["nav"].isnull().sum()
        if null_navs > 0:
            results["issues"].append(f"Null NAVs: {null_navs}")

    # Check for negative NAVs
    if "nav" in df.columns:
        negative_navs = (df["nav"] < 0).sum()
        if negative_navs > 0:
            results["issues"].append(f"Negative NAVs: {negative_navs}")

    # Check date alignment (ensure business days)
    if "date" in df.columns:
        # Check if we have weekend dates (which shouldn't happen for mutual funds)
        weekend_dates = df[df["date"].dt.weekday >= 5]
        if len(weekend_dates) > 0:
            results["issues"].append(f"Weekend dates found: {len(weekend_dates)}")

    logger.info(f"AMFI NAV validation complete. Issues found: {len(results['issues'])}")
    return results


def validate_fama_french_data(df: pd.DataFrame) -> Dict[str, any]:
    """
    Validate Fama-French factor data.

    Args:
        df: DataFrame with Fama-French factor data

    Returns:
        Dictionary with validation results
    """
    logger.info("Validating Fama-French factor data...")
    results = {
        "total_records": len(df),
        "date_range": {
            "start": str(df["date"].min()) if "date" in df.columns else None,
            "end": str(df["date"].max()) if "date" in df.columns else None,
        },
        "issues": [],
    }

    # Check for required columns
    required_columns = ["date", "mkt_rf", "smb", "hml", "rf"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        results["issues"].append(f"Missing columns: {missing_columns}")

    # Check for null values in critical columns
    critical_columns = ["date", "mkt_rf", "smb", "hml", "rf"]
    for col in critical_columns:
        if col in df.columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                results["issues"].append(f"Null {col}: {null_count}")

    logger.info(
        f"Fama-French validation complete. Issues found: {len(results['issues'])}"
    )
    return results


def validate_benchmark_data(df: pd.DataFrame) -> Dict[str, any]:
    """
    Validate benchmark data.

    Args:
        df: DataFrame with benchmark data

    Returns:
        Dictionary with validation results
    """
    logger.info("Validating benchmark data...")
    results = {
        "total_records": len(df),
        "unique_indices": df["ticker"].nunique() if "ticker" in df.columns else 0,
        "date_range": {
            "start": str(df["Date"].min()) if "Date" in df.columns else None,
            "end": str(df["Date"].max()) if "Date" in df.columns else None,
        },
        "issues": [],
    }

    # Check for required columns
    required_columns = [
        "Date",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "ticker",
        "name",
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        results["issues"].append(f"Missing columns: {missing_columns}")

    # Check for null values in critical columns
    critical_columns = ["Date", "Close"]
    for col in critical_columns:
        if col in df.columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                results["issues"].append(f"Null {col}: {null_count}")

    logger.info(
        f"Benchmark validation complete. Issues found: {len(results['issues'])}"
    )
    return results


def run_all_validations() -> Dict[str, any]:
    """
    Run all data validations and generate a report.

    Returns:
        Dictionary with validation report
    """
    logger.info("Running all data validations...")
    report = {"timestamp": pd.Timestamp.now().isoformat(), "validations": {}}

    # Validate AMFI NAV data
    try:
        nav_file = "data/raw/amfi_nav.parquet"
        if os.path.exists(nav_file):
            nav_df = pd.read_parquet(nav_file)
            report["validations"]["amfi_nav"] = validate_amfi_nav_data(nav_df)
        else:
            report["validations"]["amfi_nav"] = {"error": f"File not found: {nav_file}"}
    except Exception as e:
        report["validations"]["amfi_nav"] = {"error": str(e)}

    # Validate Fama-French data
    try:
        ff_file = "data/raw/fama_french_3factor.parquet"
        if os.path.exists(ff_file):
            ff_df = pd.read_parquet(ff_file)
            report["validations"]["fama_french"] = validate_fama_french_data(ff_df)
        else:
            report["validations"]["fama_french"] = {
                "error": f"File not found: {ff_file}"
            }
    except Exception as e:
        report["validations"]["fama_french"] = {"error": str(e)}

    # Validate benchmark data
    try:
        bench_file = "data/raw/benchmarks.parquet"
        if os.path.exists(bench_file):
            bench_df = pd.read_parquet(bench_file)
            report["validations"]["benchmark"] = validate_benchmark_data(bench_df)
        else:
            report["validations"]["benchmark"] = {
                "error": f"File not found: {bench_file}"
            }
    except Exception as e:
        report["validations"]["benchmark"] = {"error": str(e)}

    return report


def save_validation_report(
    report: Dict[str, any], output_dir: str = "data/processed/"
) -> str:
    """
    Save validation report to JSON file.

    Args:
        report: Validation report dictionary
        output_dir: Directory to save the report

    Returns:
        Path to saved report file
    """
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "validation_report.json")

    # Convert timestamps to strings for JSON serialization
    def convert_timestamps(obj):
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {k: convert_timestamps(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_timestamps(item) for item in obj]
        return obj

    serializable_report = convert_timestamps(report)

    with open(output_file, "w") as f:
        json.dump(serializable_report, f, indent=2)

    logger.info(f"Saved validation report to {output_file}")
    return output_file


def main():
    """Main function to run all validations and save report."""
    # Run validations
    report = run_all_validations()

    # Print summary
    print("Data Validation Report Summary:")
    print("=" * 40)
    for dataset, validation in report["validations"].items():
        if "error" in validation:
            print(f"{dataset}: ERROR - {validation['error']}")
        else:
            print(f"{dataset}: {validation['total_records']} records")
            if validation["issues"]:
                print(f"  Issues: {len(validation['issues'])}")
                for issue in validation["issues"]:
                    print(f"    - {issue}")
            else:
                print("  No issues found")

    # Save report
    report_file = save_validation_report(report)
    print(f"\nFull report saved to: {report_file}")


if __name__ == "__main__":
    main()
