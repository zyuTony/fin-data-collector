# Financial Data Pipeline Scripts

A collection of scripts for maintaining and updating financial market data in a PostgreSQL database.

## Core Pipeline Scripts

### Cointegration Analysis Pipelines

1. `pipeline_coin_signal_updater_coint.py`

- Updates cryptocurrency cointegration signals
- Calculates rolling cointegration for top coins by market cap
- Generates and stores trading signals
- Updates API output data

2. `pipeline_stock_signal_updater_coint.py`

- Similar functionality as coin updater but for stocks
- Processes top stocks by market cap
- Calculates stock pair cointegration metrics

3. `pipeline_stock_signal_updater_coint_by_sectors.py`

- Sector-based stock cointegration analysis
- Groups stocks by sector before analysis
- Processes top 50 stocks per sector

### Technical Analysis Pipeline

`pipeline_coin_signal_updater_stonewell.py`

- Calculates technical indicators for cryptocurrencies
- Includes RSI, moving averages, and volume metrics
- Generates trading signals based on technical analysis

## Requirements

### API Keys Required

- Alpha Vantage Premium API (for stock data)
- CoinGecko API (for cryptocurrency data)
- Database credentials in `.env` file

## How to Use

To run the financial data pipeline scripts, execute the provided shell scripts. These scripts are designed to automate the data update process and can be scheduled as cron jobs for regular execution. Ensure that the file paths and environment variables are correctly set in the scripts before running.

## Data Processing

- Processes top 300 cryptocurrencies by market cap
- Handles top 2000 stocks for general analysis
- Updates company overview information
- Calculates various trading signals and metrics

## Scheduling

- Scripts designed to run as cron jobs
- Recommended to schedule updates during market off-hours
- Different update frequencies can be set for different data types

## Output Tables

- Cointegration results
- Trading signals
- Price history
- Technical indicators
- API-ready formatted data

## Note

Premium API subscriptions recommended for production use due to rate limits on free tiers.
