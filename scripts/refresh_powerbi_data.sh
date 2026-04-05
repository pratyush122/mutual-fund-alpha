#!/bin/bash
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
