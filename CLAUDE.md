# CLAUDE.md
# Project: Mutual Fund Alpha Decomposition Tool
# Owner: Pratyush Mishra
# Phase: 1 — Data Analytics (ML integration planned for Phase 2)
# Session limit: 5 hours
# Recovery: always read .checkpoint before starting any step
# Never ask for human confirmation mid-task — proceed autonomously
# Never re-fetch data that exists in data/cache/
# IMPORTANT: Every 30 minutes, auto-save progress to 
# .checkpoint and Memory MCP without being asked.
# Commit to git every time a full step completes.
# RECOVERY RULES
# 1. Always read .checkpoint before starting any work
# 2. Always check data/cache/ before fetching any data
# 3. Auto-commit to git after every completed step
# 4. Save to Memory MCP every 30 minutes
# 5. Never drop Supabase tables — use upsert only
# 6. venv activation: source venv/bin/activate
# 7. env loading: from dotenv import load_dotenv

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a mutual fund alpha decomposition tool that analyzes mutual fund performance using the Fama-French 3-factor model to identify funds with genuine stock-picking ability. The project includes:

1. Data ingestion from AMFI (Association of Mutual Funds in India)
2. Processing of mutual fund NAV data
3. Factor model analysis using Fama-French 3-factor model
4. Bootstrap testing for statistical significance
5. Skill metrics computation
6. Streamlit dashboard for visualization
7. Power BI integration for advanced analytics
8. Supabase database for data storage

## Common Development Commands

### Running Tests
```bash
# Install test dependencies
pip install pytest statsmodels scikit-learn

# Run all tests
python -m pytest tests/ -v
```

### Running the Dashboard
```bash
# Install Streamlit
pip install streamlit

# Run the Streamlit dashboard
python -m streamlit run mutual_fund_alpha/src/dashboard/app.py
```

### Installing Dependencies
```bash
# Install core dependencies
pip install -r requirements.txt

# Install development dependencies
pip install -r requirements-dev.txt

# Install additional packages needed for analysis
pip install statsmodels scikit-learn streamlit
```

### Code Formatting and Linting
```bash
# Install formatting tools
pip install black ruff

# Check code formatting
black --check .

# Run linting
ruff check .
```

## Code Architecture

### Directory Structure
- `mutual_fund_alpha/src/`: Main source code
  - `analysis/`: Core analysis modules (factor models, bootstrap testing, skill metrics)
  - `dashboard/`: Streamlit dashboard application
  - `database/`: Database client and migration scripts
  - `exports/`: Export functionality for Power BI
  - `ingestion/`: Data ingestion modules
  - `processing/`: Data processing modules
  - `utils/`: Utility functions
- `tests/`: Unit tests
- `data/`: Data files (raw, processed, exports for Power BI)
- `docs/`: Documentation
- `scripts/`: Helper scripts

### Key Modules

1. **Factor Model Analysis** (`src/analysis/factor_model.py`)
   - Implements Fama-French 3-factor model regression
   - Computes alpha, beta coefficients, and statistical measures

2. **Bootstrap Testing** (`src/analysis/bootstrap_test.py`)
   - Performs bootstrap resampling to assess statistical significance of alpha

3. **Skill Metrics** (`src/analysis/skill_metrics.py`)
   - Computes composite skill scores and classifications

4. **Dashboard** (`src/dashboard/app.py`)
   - Streamlit application with multiple pages:
     - Fund Screener
     - Fund X-Ray Analysis
     - Category Benchmarking
     - Data Health

5. **Database Integration** (`src/database/`)
   - Client for Supabase database operations
   - Migration and seeding scripts

6. **Power BI Integration** (`src/exports/powerbi_connector.py`)
   - Exports data in formats suitable for Power BI
   - Scripts for refreshing data

### Data Flow
1. Raw data ingestion (AMFI NAV, Fama-French factors, benchmarks)
2. Data processing (returns calculation, factor alignment)
3. Statistical analysis (factor models, bootstrap testing)
4. Skill metric computation
5. Results storage in database
6. Visualization through dashboard and Power BI exports

## Troubleshooting

### Common Issues
1. **Missing dependencies**: Install all required packages with `pip install -r requirements.txt`
2. **Database connection**: Ensure Supabase credentials are properly configured
3. **Formatting issues**: Run Black formatter to fix CI/CD pipeline issues
4. **Test failures**: Make sure all statistical packages are installed

### CI/CD Pipeline
The GitHub Actions workflow runs:
1. Code formatting checks (Black)
2. Linting (Ruff)
3. Unit tests (pytest)
4. Deployment to production (on main branch)

To fix formatting issues, run:
```bash
black .
git add .
git commit -m "Fix formatting"
git push
```