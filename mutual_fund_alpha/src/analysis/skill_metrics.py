"""
Skill Metrics Computation Module
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from src.utils.logger import logger

def compute_sharpe_ratio(returns: pd.Series, risk_free_rate: float = 0.0) -> float:
    """
    Compute annualized Sharpe ratio.

    Args:
        returns: Series of returns
        risk_free_rate: Annual risk-free rate

    Returns:
        Annualized Sharpe ratio
    """
    try:
        if len(returns) == 0:
            return 0.0

        # Compute excess returns
        excess_returns = returns - risk_free_rate / 252  # Daily risk-free rate

        # Compute Sharpe ratio (annualized)
        mean_excess_return = excess_returns.mean()
        std_dev = returns.std()

        if std_dev == 0:
            return 0.0

        daily_sharpe = mean_excess_return / std_dev
        annualized_sharpe = daily_sharpe * np.sqrt(252)  # Annualize

        return annualized_sharpe

    except Exception as e:
        logger.error(f"Failed to compute Sharpe ratio: {e}")
        return 0.0

def compute_information_ratio(portfolio_returns: pd.Series, benchmark_returns: pd.Series) -> float:
    """
    Compute Information Ratio vs benchmark.

    Args:
        portfolio_returns: Series of portfolio returns
        benchmark_returns: Series of benchmark returns

    Returns:
        Information ratio
    """
    try:
        if len(portfolio_returns) == 0 or len(benchmark_returns) == 0:
            return 0.0

        # Compute active returns
        active_returns = portfolio_returns - benchmark_returns

        # Compute Information Ratio
        mean_active_return = active_returns.mean()
        tracking_error = active_returns.std()

        if tracking_error == 0:
            return 0.0

        info_ratio = mean_active_return / tracking_error

        return info_ratio

    except Exception as e:
        logger.error(f"Failed to compute Information Ratio: {e}")
        return 0.0

def compute_alpha_significance_metrics(regression_results: pd.DataFrame) -> Dict:
    """
    Compute alpha significance metrics.

    Args:
        regression_results: DataFrame with regression results

    Returns:
        Dictionary with alpha significance metrics
    """
    try:
        if len(regression_results) == 0:
            return {
                'alpha_t_stat': 0.0,
                'alpha_persistence_score': 0.0,
                'consistency_percentage': 0.0
            }

        # Average t-statistic of alpha
        avg_t_stat = regression_results['t_stat_alpha'].mean()

        # Alpha persistence (autocorrelation of rolling alphas)
        alphas = regression_results['alpha']
        if len(alphas) > 1:
            autocorr = alphas.autocorr()
            persistence_score = max(0, autocorr)  # Only positive autocorrelation is skill
        else:
            persistence_score = 0.0

        # Consistency score: % of rolling windows with positive alpha
        positive_alpha_pct = (regression_results['alpha'] > 0).mean() * 100

        return {
            'alpha_t_stat': avg_t_stat,
            'alpha_persistence_score': persistence_score,
            'consistency_percentage': positive_alpha_pct
        }

    except Exception as e:
        logger.error(f"Failed to compute alpha significance metrics: {e}")
        return {
            'alpha_t_stat': 0.0,
            'alpha_persistence_score': 0.0,
            'consistency_percentage': 0.0
        }

def compute_composite_skill_score(metrics: Dict, weights: Optional[Dict] = None) -> float:
    """
    Compute composite skill score as weighted combination of metrics.

    Args:
        metrics: Dictionary with individual metrics
        weights: Optional weights for each metric

    Returns:
        Composite skill score (0-100)
    """
    if weights is None:
        # Default weights
        weights = {
            'sharpe_ratio': 0.25,
            'info_ratio': 0.25,
            'alpha_t_stat': 0.25,
            'consistency_percentage': 0.25
        }

    try:
        # Normalize metrics to 0-100 scale where applicable
        normalized_metrics = {}

        # Sharpe ratio (good if > 0, excellent if > 2)
        sharpe = metrics.get('sharpe_ratio', 0)
        normalized_metrics['sharpe_ratio'] = min(100, max(0, sharpe * 25))  # Scale to 0-100

        # Information ratio (good if > 0, excellent if > 1)
        info_ratio = metrics.get('info_ratio', 0)
        normalized_metrics['info_ratio'] = min(100, max(0, info_ratio * 50))  # Scale to 0-100

        # Alpha t-stat (good if > 1.96, excellent if > 3)
        t_stat = metrics.get('alpha_t_stat', 0)
        normalized_metrics['alpha_t_stat'] = min(100, max(0, (t_stat / 3) * 100))  # Scale to 0-100

        # Consistency percentage (0-100 already)
        consistency = metrics.get('consistency_percentage', 0)
        normalized_metrics['consistency_percentage'] = consistency

        # Compute weighted average
        composite_score = sum(
            normalized_metrics[key] * weights[key]
            for key in weights.keys()
        )

        return composite_score

    except Exception as e:
        logger.error(f"Failed to compute composite skill score: {e}")
        return 0.0

def compute_percentile_rank(score: float, all_scores: List[float]) -> float:
    """
    Compute percentile rank of a score among all scores.

    Args:
        score: Individual score
        all_scores: List of all scores

    Returns:
        Percentile rank (0-100)
    """
    try:
        if not all_scores:
            return 50.0  # Median if no data

        # Compute percentile rank
        percentile = (sum(1 for s in all_scores if s < score) / len(all_scores)) * 100
        return percentile

    except Exception as e:
        logger.error(f"Failed to compute percentile rank: {e}")
        return 50.0

def compute_all_skill_metrics(fund_data: pd.DataFrame, benchmark_data: pd.DataFrame,
                             regression_results: pd.DataFrame) -> pd.DataFrame:
    """
    Compute all skill metrics for a set of funds.

    Args:
        fund_data: DataFrame with fund returns
        benchmark_data: DataFrame with benchmark returns
        regression_results: DataFrame with regression results

    Returns:
        DataFrame with skill metrics for each fund
    """
    logger.info("Computing skill metrics for all funds...")

    try:
        results = []
        funds = fund_data['scheme_code'].unique()

        # Collect all scores for percentile ranking
        all_composite_scores = []

        # First pass: compute individual metrics
        for fund in funds:
            fund_returns = fund_data[fund_data['scheme_code'] == fund]['daily_return']
            benchmark_returns = benchmark_data[benchmark_data['ticker'] == '^GSPC']['Close'].pct_change().dropna()

            # Compute metrics
            sharpe = compute_sharpe_ratio(fund_returns)
            info_ratio = compute_information_ratio(fund_returns, benchmark_returns)

            # Get regression results for this fund
            fund_regression = regression_results[regression_results['scheme_code'] == fund]
            alpha_metrics = compute_alpha_significance_metrics(fund_regression)

            # Compute composite score
            metrics = {
                'sharpe_ratio': sharpe,
                'info_ratio': info_ratio,
                'alpha_t_stat': alpha_metrics['alpha_t_stat'],
                'consistency_percentage': alpha_metrics['consistency_percentage']
            }
            composite_score = compute_composite_skill_score(metrics)
            all_composite_scores.append(composite_score)

            results.append({
                'scheme_code': fund,
                'sharpe_ratio': sharpe,
                'info_ratio': info_ratio,
                'alpha_t_stat': alpha_metrics['alpha_t_stat'],
                'alpha_persistence_score': alpha_metrics['alpha_persistence_score'],
                'consistency_percentage': alpha_metrics['consistency_percentage'],
                'composite_skill_score': composite_score
            })

        # Second pass: compute percentile ranks
        skill_df = pd.DataFrame(results)
        skill_df['percentile_rank'] = skill_df['composite_skill_score'].apply(
            lambda x: compute_percentile_rank(x, all_composite_scores)
        )

        logger.info(f"Computed skill metrics for {len(skill_df)} funds")
        return skill_df

    except Exception as e:
        logger.error(f"Failed to compute skill metrics: {e}")
        raise

if __name__ == "__main__":
    # Example usage
    print("Skill metrics module loaded successfully")