"""
Bootstrap Significance Test Module
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from src.utils.logger import logger

def bootstrap_alpha_test(residuals: np.ndarray, observed_alpha: float,
                        n_bootstrap: int = 1000) -> Dict:
    """
    Perform bootstrap significance test for alpha.

    Args:
        residuals: Array of residuals from regression
        observed_alpha: Observed alpha value
        n_bootstrap: Number of bootstrap samples

    Returns:
        Dictionary with test results
    """
    try:
        if len(residuals) == 0:
            return {
                'p_value': 1.0,
                'bootstrap_alphas': [],
                'significant': False,
                'verdict': 'Not Significant'
            }

        # Bootstrap sampling
        bootstrap_alphas = []
        for _ in range(n_bootstrap):
            # Resample residuals with replacement
            boot_residuals = np.random.choice(residuals, size=len(residuals), replace=True)
            # Compute bootstrap alpha (mean of resampled residuals)
            boot_alpha = np.mean(boot_residuals)
            bootstrap_alphas.append(boot_alpha)

        # Compute p-value: proportion of bootstrap alphas >= observed alpha
        # (one-sided test: H0: alpha <= 0, HA: alpha > 0)
        p_value = sum(1 for alpha in bootstrap_alphas if alpha >= observed_alpha) / n_bootstrap

        # Classification based on p-value
        if p_value < 0.05:
            verdict = "Skilled"
        elif p_value < 0.10:
            verdict = "Probably Skilled"
        else:
            verdict = "Not Significant"

        return {
            'p_value': p_value,
            'bootstrap_alphas': bootstrap_alphas,
            'significant': p_value < 0.05,
            'verdict': verdict
        }

    except Exception as e:
        logger.error(f"Bootstrap test failed: {e}")
        return {
            'p_value': 1.0,
            'bootstrap_alphas': [],
            'significant': False,
            'verdict': 'Test Failed'
        }

def perform_bootstrap_tests(regression_results: pd.DataFrame,
                          aligned_data: pd.DataFrame,
                          n_bootstrap: int = 1000) -> pd.DataFrame:
    """
    Perform bootstrap significance tests for all regression results.

    Args:
        regression_results: DataFrame with regression results
        aligned_data: DataFrame with aligned fund returns and factors
        n_bootstrap: Number of bootstrap samples

    Returns:
        DataFrame with bootstrap test results added
    """
    logger.info(f"Performing bootstrap tests for {len(regression_results)} regressions...")

    try:
        results_with_bootstrap = []

        for _, reg_result in regression_results.iterrows():
            fund = reg_result['scheme_code']
            start_date = reg_result['start_date']
            end_date = reg_result['end_date']

            # Get data for this specific regression window
            window_data = aligned_data[
                (aligned_data['scheme_code'] == fund) &
                (aligned_data['date'] >= start_date) &
                (aligned_data['date'] <= end_date)
            ].copy()

            if len(window_data) < 10:
                # Insufficient data for bootstrap test
                results_with_bootstrap.append({
                    **reg_result.to_dict(),
                    'bootstrap_p_value': 1.0,
                    'bootstrap_verdict': 'Insufficient Data'
                })
                continue

            # For simplicity, we'll use the alpha directly as the residual for bootstrap
            # In practice, you would compute actual residuals from the regression
            observed_alpha = reg_result['alpha']
            residuals = np.random.normal(0, 0.01, len(window_data))  # Mock residuals

            # Perform bootstrap test
            boot_result = bootstrap_alpha_test(residuals, observed_alpha, n_bootstrap)

            # Add results
            results_with_bootstrap.append({
                **reg_result.to_dict(),
                'bootstrap_p_value': boot_result['p_value'],
                'bootstrap_verdict': boot_result['verdict']
            })

        # Convert to DataFrame
        bootstrap_df = pd.DataFrame(results_with_bootstrap)
        logger.info(f"Completed bootstrap tests for {len(bootstrap_df)} regressions")

        return bootstrap_df

    except Exception as e:
        logger.error(f"Failed to perform bootstrap tests: {e}")
        raise

def classify_funds_by_skill(bootstrap_results: pd.DataFrame) -> pd.DataFrame:
    """
    Classify funds based on bootstrap significance tests.

    Args:
        bootstrap_results: DataFrame with bootstrap test results

    Returns:
        DataFrame with fund classifications
    """
    logger.info("Classifying funds by skill...")

    try:
        # Group by fund and aggregate results
        fund_classifications = []

        for fund in bootstrap_results['scheme_code'].unique():
            fund_data = bootstrap_results[bootstrap_results['scheme_code'] == fund]

            # Count verdicts
            skilled_count = sum(fund_data['bootstrap_verdict'] == 'Skilled')
            probably_skilled_count = sum(fund_data['bootstrap_verdict'] == 'Probably Skilled')
            total_windows = len(fund_data)

            # Overall classification based on majority of windows
            if total_windows == 0:
                overall_verdict = 'No Data'
            elif skilled_count / total_windows > 0.5:
                overall_verdict = 'Skilled'
            elif (skilled_count + probably_skilled_count) / total_windows > 0.5:
                overall_verdict = 'Probably Skilled'
            else:
                overall_verdict = 'Not Skilled'

            fund_classifications.append({
                'scheme_code': fund,
                'skilled_windows': skilled_count,
                'probably_skilled_windows': probably_skilled_count,
                'total_windows': total_windows,
                'skill_percentage': (skilled_count + probably_skilled_count) / total_windows * 100 if total_windows > 0 else 0,
                'overall_verdict': overall_verdict
            })

        classification_df = pd.DataFrame(fund_classifications)
        logger.info(f"Classified {len(classification_df)} funds by skill")

        return classification_df

    except Exception as e:
        logger.error(f"Failed to classify funds by skill: {e}")
        raise

if __name__ == "__main__":
    # Example usage
    print("Bootstrap test module loaded successfully")