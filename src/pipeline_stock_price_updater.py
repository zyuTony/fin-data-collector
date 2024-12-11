from config import *
from dotenv import load_dotenv
import os
from utils.refactor_db_data_updater import *
from utils.refactor_data_api_getter import *

load_dotenv(override=True)
bn_api_key = os.getenv('BINANCE_API')  
bn_api_secret = os.getenv('BINANCE_SECRET')  
cmc_api_key = os.getenv('CMC_API')  
avan_api_key = os.getenv('ALPHA_VANTAGE_PREM_API') 

DB_USERNAME = os.getenv('RDS_USERNAME') 
DB_PASSWORD = os.getenv('RDS_PASSWORD') 
DB_HOST = os.getenv('RDS_ENDPOINT') 
DB_NAME = 'financial_data'

'''
Stock Price Refresh Pipeline
Cadence: AUTOMATIC DAILY
  1. Download stock price json from AVAN
  2. Insert data into DB
'''
# download json
etfs = ["SPY",   # SPDR S&P 500 ETF - Large-Cap U.S. Stocks
    "VTI",   # Vanguard Total Stock Market ETF - Total U.S. Stock Market
    "IWM",   # iShares Russell 2000 ETF - Small-Cap U.S. Stocks
    "EEM",   # iShares MSCI Emerging Markets ETF - Emerging Markets Stocks
    "VEA",   # Vanguard FTSE Developed Markets ETF - Developed International Markets
    "QQQ",   # Invesco QQQ Trust - Nasdaq-100 (Large-Cap Growth)
    "VNQ",   # Vanguard Real Estate ETF - Real Estate (REITs)
    "EFA",   # iShares MSCI EAFE ETF - International Developed Markets
    "BND",   # Vanguard Total Bond Market ETF - U.S. Investment-Grade Bonds
    "GLD",   # SPDR Gold Shares - Gold
    "XLK",   # Technology Select Sector SPDR Fund - Technology Sector
    "XLF",   # Financial Select Sector SPDR Fund - Financial Sector
    "XLE",   # Energy Select Sector SPDR Fund - Energy Sector
    "XLY",   # Consumer Discretionary Select Sector SPDR Fund - Consumer Discretionary Sector
    "XLP",   # Consumer Staples Select Sector SPDR Fund - Consumer Staples Sector
    "XLV",   # Health Care Select Sector SPDR Fund - Healthcare Sector
    "XLU",   # Utilities Select Sector SPDR Fund - Utilities Sector
    "XLI",   # Industrial Select Sector SPDR Fund - Industrials Sector
    "XLB",   # Materials Select Sector SPDR Fund - Materials Sector
    "SCHH",  # Schwab U.S. REIT ETF - Real Estate (REITs)
    "TIP",   # iShares TIPS Bond ETF - U.S. Treasury Inflation-Protected Securities (TIPS)
    "AGG",   # iShares Core U.S. Aggregate Bond ETF - U.S. Aggregate Bond Market
    "LQD",   # iShares iBoxx $ Investment Grade Corporate Bond ETF - Investment-Grade Corporate Bonds
    "TLT",   # iShares 20+ Year Treasury Bond ETF - Long-Term U.S. Treasury Bonds
    "HYG",   # iShares iBoxx $ High Yield Corporate Bond ETF - High Yield Corporate Bonds
    "IJH",   # iShares Core S&P Mid-Cap ETF - Mid-Cap U.S. Stocks
    "IEMG",  # iShares Core MSCI Emerging Markets ETF - Emerging Markets
    "VWO",   # Vanguard FTSE Emerging Markets ETF - Emerging Markets
    "VOO",   # Vanguard S&P 500 ETF - S&P 500 Index
    "XRT",   # SPDR S&P Retail ETF - Retail Sector
    "XBI",   # SPDR S&P Biotech ETF - Biotech Sector
    "IYR",   # iShares U.S. Real Estate ETF - U.S. Real Estate Sector
    "XOP",   # SPDR S&P Oil & Gas Exploration & Production ETF - Oil & Gas Sector
    "DIA",   # SPDR Dow Jones Industrial Average ETF - Dow Jones Industrial Average
    "VUG",   # Vanguard Growth ETF - U.S. Growth Stocks
    "VTV",   # Vanguard Value ETF - U.S. Value Stocks
    "MTUM",  # iShares MSCI USA Momentum Factor ETF - U.S. Momentum Stocks
    "USMV",  # iShares MSCI USA Minimum Volatility ETF - U.S. Minimum Volatility Stocks
    "ITOT",  # iShares Core S&P Total U.S. Stock Market ETF - Total U.S. Stock Market
    "MCHI",  # iShares MSCI China ETF - Chinese Stocks
    "EWJ",   # iShares MSCI Japan ETF - Japanese Stocks
    "EWG",   # iShares MSCI Germany ETF - German Stocks
    "EWZ",   # iShares MSCI Brazil ETF - Brazilian Stocks
    "EWT",   # iShares MSCI Taiwan ETF - Taiwanese Stocks
    "FXI",   # iShares China Large-Cap ETF - Chinese Large-Cap Stocks
    "RSX",   # VanEck Russia ETF - Russian Stocks
    "EWY",   # iShares MSCI South Korea ETF - South Korean Stocks
    "VEU",   # Vanguard FTSE All-World ex-US ETF - International Stocks ex-US
    "VT",    # Vanguard Total World Stock ETF - Global Stocks
    "XHB",   # SPDR S&P Homebuilders ETF - Homebuilders Sector
    "SLV",   # iShares Silver Trust - Silver
    "PFF",   # iShares Preferred and Income Securities ETF - Preferred Stocks
    "TLH",   # iShares 10-20 Year Treasury Bond ETF - 10-20 Year U.S. Treasury Bonds
    "IEF",   # iShares 7-10 Year Treasury Bond ETF - 7-10 Year U.S. Treasury Bonds
    "SHY",   # iShares 1-3 Year Treasury Bond ETF - Short-Term U.S. Treasury Bonds
    "MUB",   # iShares National Muni Bond ETF - U.S. Municipal Bonds
    'KWEB'
]
api_getter = avan_stock_daily_ohlc_api_getter(api_key=avan_api_key,
                                              data_save_path=AVAN_DAILY_JSON_PATH,
                                              start_date=None,# not applicable
                                              end_date=None,# not applicable
                                              additional_tickers=etfs)
api_getter.download_data() 

# insert to db
db = avan_stock_OHLC_db_refresher(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD, "stock_historical_price")
db.connect()
db.create_table()
for filename in os.listdir(AVAN_DAILY_JSON_PATH):
    if filename.endswith('.json'):
        file_path = os.path.join(AVAN_DAILY_JSON_PATH, filename)
        db.insert_data(file_path)
db.close()
 