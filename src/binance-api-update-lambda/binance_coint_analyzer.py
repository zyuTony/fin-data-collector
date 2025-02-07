from config import *
from dotenv import load_dotenv
import os
from db_helper_functions import db_refresher, binance_cointegration_db_refresher
import pandas as pd
from statsmodels.tsa.stattools import coint
import logging

def lambda_handler(event, context):
    # environment variables and initialize clients
    load_dotenv(override=True)  
    DB_NAME = os.getenv('DB_NAME')
    DB_HOST = os.getenv('DB_HOST')
    DB_USERNAME = os.getenv('DB_USERNAME')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    

    group1_symbols = event.get('group1_symbols', ["BTC", "ETH", "SOL", "DOGE", "PEPE", "XRP", "BNB", "SHIB", "AVAX", "SUI", "ADA", "NEAR", "FLOKI", "LINK", "FET",
    "SEI", "OP", "FIL", "FTM", "LTC", "PEOPLE", "INJ", "DOT", "TRX", "APT"]) 
    
    group2_symbols = event.get('group2_symbols', [
    "BTC", "ETH", "SOL", "DOGE", "PEPE", "XRP", "BNB", "SHIB",
    "AVAX", "SUI", "ARB", "ADA", "WLD", "NEAR", "FLOKI", "RUNE", "LINK", "FET",
    "SEI", "OP", "FIL", "FTM", "LTC", "PEOPLE", "INJ", "DOT", "TRX", "APT",
    "BCH", "GALA", "ICP", "UNI", "TRB", "ETC", "STX", "LUNC",
    "ENS", "XLM", "ARKM", "HBAR", "ATOM", "PENDLE", "DYDX", "AAVE", "JASMY", "LDO",
    "FTT", "AR", "CRV", "CKB", "LUNA", "OM"
    ]) 
    
    # group1_symbols = event.get('group1_symbols', ["BTC", "ETH", "SOL"]) 
    
    # group2_symbols = event.get('group2_symbols', ["BTC", "ETH", "SOL", "DOGE", "PEPE", "XRP"]) 
    
    window_size = event.get('window_size', 60)
    
    # get market data from db
    db = db_refresher(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD, "binance_market_data")
    db.connect()
    market_data = db.get_data(columns=["date", "symbol", "close"])
    db.close()
    
    def _single_pair_rolling_coint(data1, data2, window_size):
        '''
        Calculate rolling cointegration between two time series
        '''
        rolling_p_values = []
        for end in range(window_size, len(data1)):
            start = end - window_size
            series1 = data1[start:end]
            series2 = data2[start:end]
            
            # Check for NaN values in the window
            if series1.isna().any() or series2.isna().any():
                rolling_p_values.append(-1)
            else:
                _, p_value, _ = coint(series1, series2, trend='ct')
                rolling_p_values.append(p_value)

        return rolling_p_values

    def calculate_group_cointegration(
        market_data_pivot: pd.DataFrame, 
        group1_symbols: list[str] = None, 
        group2_symbols: list[str] = None, 
        window_size: int = 60
    ) -> pd.DataFrame:
        """
        Calculate cointegration between two groups of symbols
        """
        # If groups not specified, use all symbols for both groups
        if group1_symbols is None:
            group1_symbols = market_data_pivot.columns.tolist()
        if group2_symbols is None:
            group2_symbols = market_data_pivot.columns.tolist()
        
        # Validate symbols exist in data
        missing_symbols = [sym for sym in group1_symbols + group2_symbols 
                            if sym not in market_data_pivot.columns]
        if missing_symbols:
            raise ValueError(f"Symbols not found in data: {missing_symbols}")
            
        # get date that needed updates
        db = db_refresher(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD, "binance_analyzer_cointegration")
        db.connect()
        existing_coint_data = db.get_data(columns=["symbol_one", "symbol_two", "date", "window_size"])
        if existing_coint_data is None:
            existing_coint_data = pd.DataFrame(columns=["symbol_one", "symbol_two", "date", "window_size", "coint_p_value"])
        db.close()
        
        results = []
        processed_pairs = set()
        for sym1 in group1_symbols:
            for sym2 in group2_symbols:
                # Skip if same symbol or if pair already processed
                if sym1 == sym2:
                    continue
                    
                pair = tuple(sorted([sym1, sym2]))
                if pair in processed_pairs:
                    continue
                processed_pairs.add(pair)
                
                logging.info(f"Processing cointegration for pair: {sym1}-{sym2}")
                
                # Get latest date for this symbol pair
                pair_data = existing_coint_data[
                    (existing_coint_data['symbol_one'] == sym1) & 
                    (existing_coint_data['symbol_two'] == sym2)
                ]
                
                # If pair exists in database, only calculate from latest date - window_size
                # If pair doesn't exist, calculate for all dates
                if len(pair_data) > 0:
                    latest_date = pd.to_datetime(pair_data['date'].max())
                    start_date = latest_date - pd.Timedelta(days=window_size) 
                    data1 = market_data_pivot[sym1][market_data_pivot.index >= start_date.date()]
                    data2 = market_data_pivot[sym2][market_data_pivot.index >= start_date.date()]
                    logging.info(f"Updating existing pair {sym1}-{sym2} from {latest_date}")
                else:
                    # For new pairs, use all available data
                    data1 = market_data_pivot[sym1]
                    data2 = market_data_pivot[sym2]
                    logging.info(f"Processing new pair {sym1}-{sym2} for all available dates")
                
                p_values = _single_pair_rolling_coint(
                    data1,
                    data2,
                    window_size
                )
                
                dates = data1.index[window_size:]
                for date, p_value in zip(dates, p_values):
                    # For existing pairs, only add new dates
                    # For new pairs, add all dates
                    if len(pair_data) == 0 or date > latest_date.date():
                        results.append({
                            'symbol1': sym1,
                            'symbol2': sym2,
                            'date': date,
                            'window_size': window_size,
                            'p_value': p_value
                        })
                
                logging.info(f"Completed processing pair {sym1}-{sym2}")
        
        if not results:
            return pd.DataFrame()
            
        results_df = pd.DataFrame(results)
        results_df['date'] = pd.to_datetime(results_df['date'])
        return results_df
    
    # Prepare market data transformation
    market_data_pivot = market_data.pivot(index='date', columns='symbol', values='close')
    market_data_pivot = market_data_pivot.sort_index()
    
    # run
    results_df = calculate_group_cointegration(market_data_pivot, group1_symbols=group1_symbols, group2_symbols=group2_symbols, window_size=window_size)
    
    # update
    db = binance_cointegration_db_refresher(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD, "binance_analyzer_cointegration")
    db.connect()
    db.create_table()
    db.insert_data(results_df)
    
    
if __name__ == "__main__":
    lambda_handler({}, {})
