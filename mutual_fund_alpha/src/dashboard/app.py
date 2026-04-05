"""
Streamlit Dashboard Application
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List
import os

# Set page configuration
st.set_page_config(
    page_title="Mutual Fund Alpha Analysis",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


def load_data() -> Dict[str, pd.DataFrame]:
    """Load all processed data for the dashboard."""
    data = {}

    # Load skill metrics
    skill_file = "data/processed/skill_metrics.parquet"
    if os.path.exists(skill_file):
        data["skill_metrics"] = pd.read_parquet(skill_file)

    # Load fund classifications
    classification_file = "data/processed/fund_classifications.parquet"
    if os.path.exists(classification_file):
        data["fund_classifications"] = pd.read_parquet(classification_file)

    # Load regression results
    regression_file = "data/processed/regression_results.parquet"
    if os.path.exists(regression_file):
        data["regression_results"] = pd.read_parquet(regression_file)

    # Load bootstrap results
    bootstrap_file = "data/processed/bootstrap_results.parquet"
    if os.path.exists(bootstrap_file):
        data["bootstrap_results"] = pd.read_parquet(bootstrap_file)

    return data


def fund_screener_page(data: Dict[str, pd.DataFrame]) -> None:
    """Display the Fund Screener page."""
    st.title("🎯 Fund Screener")

    if "skill_metrics" not in data:
        st.warning("No skill metrics data available")
        return

    df = data["skill_metrics"].copy()

    # Merge with classifications if available
    if "fund_classifications" in data:
        df = pd.merge(
            df,
            data["fund_classifications"][["scheme_code", "overall_verdict"]],
            on="scheme_code",
            how="left",
        )

    # Sidebar filters
    st.sidebar.header("Filters")

    # Verdict filter
    verdict_options = ["All"] + sorted(df["overall_verdict"].dropna().unique().tolist())
    selected_verdict = st.sidebar.selectbox(
        "Skill Classification",
        verdict_options,
        index=0 if "All" in verdict_options else 0,
    )

    # Minimum skill score filter
    min_skill_score = st.sidebar.slider("Minimum Skill Score", 0, 100, 0)

    # Apply filters
    filtered_df = df[df["composite_skill_score"] >= min_skill_score]

    if selected_verdict != "All":
        filtered_df = filtered_df[filtered_df["overall_verdict"] == selected_verdict]

    # Display metrics table
    st.subheader("Fund Metrics")

    # Select columns to display
    display_columns = [
        "scheme_code",
        "composite_skill_score",
        "sharpe_ratio",
        "info_ratio",
        "percentile_rank",
        "overall_verdict",
    ]

    # Rename columns for display
    column_names = {
        "scheme_code": "Fund",
        "composite_skill_score": "Skill Score",
        "sharpe_ratio": "Sharpe Ratio",
        "info_ratio": "Info Ratio",
        "percentile_rank": "Percentile",
        "overall_verdict": "Verdict",
    }

    display_df = filtered_df[display_columns].rename(columns=column_names)

    # Style the verdict column
    def style_verdict(val):
        if val == "Skilled":
            color = "green"
        elif val == "Probably Skilled":
            color = "orange"
        elif val == "Not Skilled":
            color = "red"
        else:
            color = "gray"
        return f"background-color: {color}; color: white"

    styled_df = display_df.style.applymap(style_verdict, subset=["Verdict"])

    st.dataframe(styled_df, use_container_width=True)

    # Download button
    csv = display_df.to_csv(index=False)
    st.download_button(
        label="Download Filtered Results",
        data=csv,
        file_name="fund_screener_results.csv",
        mime="text/csv",
    )


def fund_xray_page(data: Dict[str, pd.DataFrame]) -> None:
    """Display the Fund X-Ray page."""
    st.title("🔍 Fund X-Ray Analysis")

    if "skill_metrics" not in data or "regression_results" not in data:
        st.warning("Required data not available for analysis")
        return

    # Fund selector
    funds = sorted(data["skill_metrics"]["scheme_code"].unique())
    selected_fund = st.selectbox("Select a Fund", funds)

    if not selected_fund:
        return

    # Get fund data
    fund_skill = data["skill_metrics"][
        data["skill_metrics"]["scheme_code"] == selected_fund
    ].iloc[0]
    fund_regression = data["regression_results"][
        data["regression_results"]["scheme_code"] == selected_fund
    ]

    # Display fund summary
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Skill Score", f"{fund_skill['composite_skill_score']:.1f}")
    with col2:
        st.metric("Sharpe Ratio", f"{fund_skill['sharpe_ratio']:.3f}")
    with col3:
        st.metric("Info Ratio", f"{fund_skill['info_ratio']:.3f}")
    with col4:
        st.metric("Percentile Rank", f"{fund_skill['percentile_rank']:.1f}%")

    # Rolling alpha chart
    st.subheader("Rolling Alpha Analysis")

    if not fund_regression.empty:
        fig_alpha = px.line(
            fund_regression,
            x="start_date",
            y="alpha",
            title=f"Rolling Alpha for {selected_fund}",
            labels={"alpha": "Alpha", "start_date": "Date"},
        )
        fig_alpha.add_hline(y=0, line_dash="dash", line_color="red")
        st.plotly_chart(fig_alpha, use_container_width=True)

        # Factor exposure chart
        st.subheader("Factor Exposures")

        # Calculate average factor exposures
        avg_beta_mkt = fund_regression["beta_mkt"].mean()
        avg_beta_smb = fund_regression["beta_smb"].mean()
        avg_beta_hml = fund_regression["beta_hml"].mean()

        factor_data = pd.DataFrame(
            {
                "Factor": ["Market (MKT)", "Size (SMB)", "Value (HML)"],
                "Exposure": [avg_beta_mkt, avg_beta_smb, avg_beta_hml],
            }
        )

        fig_factors = px.bar(
            factor_data,
            x="Factor",
            y="Exposure",
            title=f"Average Factor Exposures for {selected_fund}",
            color="Exposure",
            color_continuous_scale="RdBu",
        )
        fig_factors.update_layout(yaxis_title="Beta Exposure")
        st.plotly_chart(fig_factors, use_container_width=True)

    # Skill scorecard
    st.subheader("Skill Scorecard")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Alpha t-statistic", f"{fund_skill['alpha_t_stat']:.2f}")
    with col2:
        st.metric("Consistency %", f"{fund_skill['consistency_percentage']:.1f}%")
    with col3:
        # Alpha persistence would be computed from autocorrelation
        st.metric("Alpha Persistence", "N/A")
    with col4:
        # Would compute from bootstrap results
        st.metric("Bootstrap Verdict", fund_skill.get("overall_verdict", "N/A"))


def category_benchmarking_page(data: Dict[str, pd.DataFrame]) -> None:
    """Display the Category Benchmarking page."""
    st.title("📈 Category Benchmarking")

    if "skill_metrics" not in data:
        st.warning("No skill metrics data available")
        return

    df = data["skill_metrics"].copy()

    # Alpha distribution by skill classification
    st.subheader("Alpha Distribution by Skill Classification")

    if "fund_classifications" in data:
        df = pd.merge(
            df,
            data["fund_classifications"][["scheme_code", "overall_verdict"]],
            on="scheme_code",
            how="left",
        )

        fig_box = px.box(
            df,
            x="overall_verdict",
            y="composite_skill_score",
            title="Skill Score Distribution by Classification",
            labels={
                "overall_verdict": "Classification",
                "composite_skill_score": "Skill Score",
            },
        )
        st.plotly_chart(fig_box, use_container_width=True)

    # Scatter plot: Skill Score vs Consistency
    st.subheader("Skill vs Consistency")

    fig_scatter = px.scatter(
        df,
        x="consistency_percentage",
        y="composite_skill_score",
        hover_data=["scheme_code"],
        title="Skill Score vs Consistency Percentage",
        labels={
            "consistency_percentage": "Consistency %",
            "composite_skill_score": "Skill Score",
        },
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

    # Category averages table
    st.subheader("Category Averages")

    # Group by verdict and compute averages
    if "overall_verdict" in df.columns:
        category_avg = (
            df.groupby("overall_verdict")
            .agg(
                {
                    "composite_skill_score": "mean",
                    "sharpe_ratio": "mean",
                    "info_ratio": "mean",
                    "consistency_percentage": "mean",
                }
            )
            .round(3)
        )

        category_avg.index.name = "Classification"
        category_avg.reset_index(inplace=True)

        st.dataframe(category_avg, use_container_width=True)


def data_health_page(data: Dict[str, pd.DataFrame]) -> None:
    """Display the Data Health page."""
    st.title("🛡️ Data Health")

    # Validation report
    validation_file = "data/processed/validation_report.json"
    if os.path.exists(validation_file):
        import json

        with open(validation_file, "r") as f:
            validation_report = json.load(f)

        st.subheader("Validation Report Summary")

        # Display timestamp
        st.write(f"Report generated: {validation_report.get('timestamp', 'Unknown')}")

        # Display validation results for each dataset
        for dataset_name, dataset_info in validation_report.get(
            "validations", {}
        ).items():
            st.markdown(f"#### {dataset_name.capitalize()}")
            if "error" in dataset_info:
                st.error(f"Error: {dataset_info['error']}")
            else:
                st.write(f"- Records: {dataset_info.get('total_records', 'N/A')}")
                st.write(
                    f"- Date range: {dataset_info.get('date_range', {}).get('start', 'N/A')} to {dataset_info.get('date_range', {}).get('end', 'N/A')}"
                )

                if dataset_info.get("issues"):
                    st.warning("Issues found:")
                    for issue in dataset_info["issues"]:
                        st.write(f"  - {issue}")
                else:
                    st.success("No issues found")

    else:
        st.warning("Validation report not found")

    # Data freshness
    st.subheader("Data Freshness")

    # Check modification times of key files
    key_files = [
        "data/raw/amfi_nav.parquet",
        "data/raw/fama_french_3factor.parquet",
        "data/raw/benchmarks.parquet",
        "data/processed/fund_returns.parquet",
    ]

    freshness_data = []
    for file_path in key_files:
        if os.path.exists(file_path):
            mod_time = os.path.getmtime(file_path)
            from datetime import datetime

            mod_datetime = datetime.fromtimestamp(mod_time)
            freshness_data.append(
                {
                    "File": file_path.replace("data/", ""),
                    "Last Modified": mod_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

    if freshness_data:
        freshness_df = pd.DataFrame(freshness_data)
        st.dataframe(freshness_df, use_container_width=True)
    else:
        st.info("No data files found for freshness check")


def main() -> None:
    """Main Streamlit application."""
    st.title("📊 Mutual Fund Alpha Decomposition Tool")

    # Load all data
    data = load_data()

    # Navigation
    pages = {
        "Fund Screener": fund_screener_page,
        "Fund X-Ray": fund_xray_page,
        "Category Benchmarking": category_benchmarking_page,
        "Data Health": data_health_page,
    }

    st.sidebar.title("Navigation")
    selected_page = st.sidebar.radio("Go to", list(pages.keys()))

    # Display selected page
    pages[selected_page](data)

    # Footer
    st.sidebar.markdown("---")
    st.sidebar.info("""
        **About this tool:**
        This dashboard analyzes mutual fund performance using the Fama-French 3-factor model
        to identify funds with genuine stock-picking ability.
    """)


if __name__ == "__main__":
    main()
