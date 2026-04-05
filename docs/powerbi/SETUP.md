# Power BI Integration Setup Guide

This guide explains how to set up Power BI integration for the Mutual Fund Alpha Decomposition Tool.

## PHASE A — DATA EXPORT LAYER

### CSV Export Setup

1. **Run the export script**:
   ```bash
   python mutual_fund_alpha/src/exports/powerbi_connector.py
   ```

2. **Generated CSV files**:
   - `data/powerbi/funds.csv`
   - `data/powerbi/nav_history.csv`
   - `data/powerbi/alpha_results.csv`
   - `data/powerbi/factor_data.csv`
   - `data/powerbi/category_benchmarks.csv`

3. **Auto-refresh script**:
   - Location: `scripts/refresh_powerbi_data.sh`
   - Runs daily to regenerate all CSVs from Supabase

### Setting up Scheduled Refresh

#### On Linux/Mac:
```bash
# Add to crontab
crontab -e
# Add this line to run daily at 2 AM:
0 2 * * * /path/to/scripts/refresh_powerbi_data.sh
```

#### On Windows:
1. Open Task Scheduler
2. Create Basic Task
3. Name: "Power BI Data Refresh"
4. Trigger: Daily at 2:00 AM
5. Action: Start a program
   - Program: `C:\path\to\your\script\refresh_powerbi_data.sh`
6. Finish

## PHASE B — POWER BI REPORT TEMPLATE

### Loading the Template

1. Open Power BI Desktop
2. Click "Get Data" → "File" → "Power BI Template (.pbit)"
3. Navigate to `docs/powerbi/mutual_fund_alpha.pbit`
4. Click "Open"

### Connecting to Data Source

1. When prompted, browse to the `data/powerbi/` folder
2. Select all 5 CSV files
3. Click "Load"

## PHASE C — DAX MEASURES

### Manual Measures to Add

Add these measures in the Power BI model:

```dax
Annualised Alpha = AVERAGE(alpha_results[alpha]) * 252

Skill Rate = DIVIDE(COUNTIF(alpha_results[verdict],"Skilled"), COUNTROWS(alpha_results))

Avg Sharpe = AVERAGE(alpha_results[sharpe])

Total Funds = COUNTROWS(funds)

Avg Info Ratio = AVERAGE(alpha_results[info_ratio])

Consistency Rate = AVERAGE(alpha_results[consistency_percentage])
```

## PHASE D — PUBLISHING TO POWER BI SERVICE

### Publishing Steps

1. In Power BI Desktop, click "Publish"
2. Sign in to Power BI Service (free tier works)
3. Select workspace (e.g., "My Workspace")
4. Click "Select"

### Setting up Gateway (if needed)

For automatic refresh from local data:

1. Download and install Power BI Gateway
2. Configure gateway in Power BI Service
3. Map local data sources to gateway
4. Set refresh schedule in dataset settings

## TROUBLESHOOTING

### Common Issues

1. **CSV files not found**:
   - Run the export script manually
   - Check that `data/powerbi/` directory exists

2. **Connection errors**:
   - Verify file paths in Power BI Desktop
   - Ensure CSV files are not locked by other programs

3. **Scheduled refresh not working**:
   - Check cron/task scheduler logs
   - Verify script has execute permissions
   - Test script manually: `./scripts/refresh_powerbi_data.sh`

### Data Update Frequency

- CSV files are regenerated daily by the refresh script
- Power BI Service can refresh every 24 hours (free tier limitation)
- For real-time data, consider upgrading to Power BI Pro

## MAINTENANCE

### Monitoring

1. Check `data/powerbi/` directory for updated files
2. Monitor Power BI Service refresh history
3. Review error logs in `logs/` directory

### Updates

When updating the tool:

1. Pull latest code
2. Run export script to test new data format
3. Update Power BI template if needed
4. Republish to Power BI Service

## SUPPORT

For issues with Power BI integration:

1. Check the logs in `logs/` directory
2. Verify all dependencies are installed
3. Ensure Supabase connection is working
4. Contact support if problems persist