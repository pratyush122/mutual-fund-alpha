"""
Fama-French 3-Factor Regression Model Module
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
import statsmodels.api as sm
from dataclasses import dataclass
from src.utils.logger import logger

@dataclass
class FactorResult:
    """Results from factor regression analysis."""
    alpha: float
    beta_mkt: float
    beta_smb: float
    beta_hml: float
    r_squared: float
    t_stat_alpha: float
    p_value_alpha: float
    residual_std_err: float
    n_observations: int

def run_factor_regression(excess_returns: pd.Series, factors: pd.DataFrame,
                          window: int = 756) -> FactorResult:
    """
    Run Fama-French 3-factor regression: R_excess = alpha + b1*MKT + b2*SMB + b3*HML + e

    Args:
        excess_returns: Series of excess returns
        factors: DataFrame with factor data (mkt_rf, smb, hml)
        window: Rolling window size in days (default: 756 for 3 years)

    Returns:
        FactorResult object with regression results
    """
    try:
        # Align data
        aligned_data = pd.concat([excess_returns, factors], axis=1).dropna()

        if len(aligned_data) < 10:  # Minimum observations needed
            logger.warning(f"Insufficient data for regression: {len(aligned_data)} observations")
            return FactorResult(
                alpha=0.0, beta_mkt=0.0, beta_smb=0.0, beta_hml=0.0,
                r_squared=0.0, t_stat_alpha=0.0, p_value_alpha=1.0,
                residual_std_err=0.0, n_observations=len(aligned_data)
            )

        # Prepare regression data
        y = aligned_data.iloc[:, 0]  # Excess returns
        X = aligned_data[['mkt_rf', 'smb', 'hml']]
        X = sm.add_constant(X)  # Add intercept

        # Run regression
        model = sm.OLS(y, X)
        results = model.fit()

        # Extract results
        alpha = results.params['const']
        beta_mkt = results.params['mkt_rf']
        beta_smb = results.params['smb']
        beta_hml = results.params['hml']
        r_squared = results.rsquared
        t_stat_alpha = results.tvalues['const']
        p_value_alpha = results.pvalues['const']
        residual_std_err = np.sqrt(results.mse_resid)

        return FactorResult(
            alpha=alpha,
            beta_mkt=beta_mkt,
            beta_smb=beta_smb,
            beta_hml=beta_hml,
            r_squared=r_squared,
            t_stat_alpha=t_stat_alpha,
            p_value_alpha=p_value_alpha,
            residual_std_err=residual_std_err,
            n_observations=len(aligned_data)
        )

    except Exception as e:
        logger.error(f"Factor regression failed: {e}")
        raise

def run_rolling_regressions(excess_returns: pd.DataFrame, factors: pd.DataFrame,
                            window: int = 756, step: int = 63) -> List[Dict]:
    """
    Run rolling factor regressions with specified window and step size.

    Args:
        excess_returns: DataFrame with excess returns (scheme_code, date, excess_return)
        factors: DataFrame with factor data
        window: Rolling window size in days
        step: Step size in days

    Returns:
        List of dictionaries with regression results for each fund and window
    """
    logger.info(f"Running rolling regressions: window={window}, step={step}")

    try:
        results = []
        funds = excess_returns['scheme_code'].unique()

        for fund in funds:
            fund_data = excess_returns[excess_returns['scheme_code'] == fund].copy()
            fund_data = fund_data.sort_values('date').reset_index(drop=True)

            # Run regressions at specified intervals
            for i in range(0, len(fund_data) - window + 1, step):
                # Get window data
                window_end = i + window
                if window_end > len(fund_data):
                    break

                window_data = fund_data.iloc[i:window_end]
                window_dates = window_data['date']

                # Get corresponding factor data
                factor_window = factors[factors['date'].isin(window_dates)]

                if len(factor_window) >= 10:  # Minimum observations
                    # Run regression
                    excess_return_series = window_data.set_index('date')['excess_return']
                    factor_data = factor_window.set_index('date')[['mkt_rf', 'smb', 'hml']]

                    try:
                        reg_result = run_factor_regression(excess_return_series, factor_data, window)

                        # Store results
                        results.append({
                            'scheme_code': fund,
                            'start_date': window_data['date'].iloc[0],
                            'end_date': window_data['date'].iloc[-1],
                            'window_days': window,
                            'alpha': reg_result.alpha,
                            'beta_mkt': reg_result.beta_mkt,
                            'beta_smb': reg_result.beta_smb,
                            'beta_hml': reg_result.beta_hml,
                            'r_squared': reg_result.r_squared,
                            't_stat_alpha': reg_result.t_stat_alpha,
                            'p_value_alpha': reg_result.p_value_alpha,
                            'residual_std_err': reg_result.residual_std_err,
                            'n_observations': reg_result.n_observations
                        })
                    except Exception as e:
                        logger.warning(f"Regression failed for {fund} window {i}: {e}")
                        continue

        logger.info(f"Completed {len(results)} rolling regressions for {len(funds)} funds")
        return results

    except Exception as e:
        logger.error(f"Rolling regressions failed: {e}")
        raise

if __name__ == "__main__":
    # Example usage
    print("Factor model module loaded successfully")