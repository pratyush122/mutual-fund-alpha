# Mutual Fund Alpha Decomposition Tool

This tool analyzes mutual fund performance by decomposing returns into systematic risk factors and true alpha. It uses the Fama-French 3-factor model to identify funds with genuine stock-picking ability versus those simply benefiting from market movements.

## Features

- Fetches mutual fund NAV data from AMFI
- Downloads Fama-French factor data
- Computes factor exposures and risk-adjusted returns
- Identifies funds with statistically significant alpha
- Provides interactive dashboard for analysis

## Architecture

```
mutual_fund_alpha/
├── data/
│   ├── raw/          # Immutable source data
│   ├── processed/    # Cleaned, transformed data
│   └── cache/        # API response cache
├── src/
│   ├── ingestion/    # Data fetchers
│   ├── processing/   # Transformers, feature engineering
│   ├── analysis/     # Factor models, statistics
│   ├── database/     # Supabase client, schema, migrations
│   ├── dashboard/    # Streamlit pages
│   └── utils/        # Logger, retry decorator, checkpoint manager
├── tests/
├── notebooks/        # Exploratory analysis
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
└── main.py           # Entry point
```

## Setup

1. Create virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up environment variables:
   ```bash
   cp .env.example .env
   # Edit .env with your API keys
   ```

4. Run the pipeline:
   ```bash
   python main.py
   ```

## Data Sources

- AMFI: Mutual fund NAV data
- Ken French: Fama-French factor data
- Yahoo Finance: Benchmark indices

## Running the Dashboard

To run the Streamlit dashboard:

```bash
streamlit run mutual_fund_alpha/src/dashboard/app.py
```

Or using Docker:

```bash
docker-compose up
```

## Running Tests

To run the test suite:

```bash
pytest tests/ -v
```

## CI/CD

The project includes GitHub Actions for continuous integration:
- Linting with Black and Ruff
- Unit tests with Pytest
- Automatic deployment on push to main branch

## ML Roadmap

Phase 2 will extend this tool with machine learning capabilities:
- Predictive modeling for fund performance
- Advanced feature engineering
- Ensemble methods for improved accuracy
- Real-time prediction APIs

## License

MIT