from config import *
from dotenv import load_dotenv
import os
from db_helper_functions import *
from db_helper_functions import db_refresher, binance_cointegration_db_refresher
import pandas as pd
from statsmodels.tsa.stattools import coint

def lambda_handler(event, context):
    # environment variables and initialize clients
    load_dotenv(override=True)  
    DB_NAME = os.getenv('DB_NAME')
    DB_HOST = os.getenv('DB_HOST')
    DB_USERNAME = os.getenv('DB_USERNAME')
    DB_PASSWORD = os.getenv('DB_PASSWORD')    

    group1_symbols = event.get('group1_symbols')
    print(f"group1_symbols: {group1_symbols}")
    
    group2_symbols = event.get('group2_symbols')
    print(f"group2_symbols: {group2_symbols}")
    window_size = event.get('window_size', 60)
    
    # get market data from db
    db = db_refresher(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD, "binance_market_data")
    db.connect()
    symbols = group1_symbols + group2_symbols
    where_clause = f"symbol IN ({', '.join([f'\'{symbol}\'' for symbol in symbols])})"
    market_data = db.get_data(columns=["date", "symbol", "close"], where_clause=where_clause)
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
        # If groups not specified, exit lambda function and return error
        if group1_symbols is None or group2_symbols is None:
            return {
                'statusCode': 400,
                'body': 'No symbols selected for group1 or group2'
            }
        
        # Validate symbols exist in data
        missing_symbols = [sym for sym in group1_symbols + group2_symbols 
                            if sym not in market_data_pivot.columns]
        if missing_symbols:
            raise ValueError(f"Symbols not found in data: {missing_symbols}")
            
        # get date that needed updates
        db = db_refresher(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD, "binance_analyzer_cointegration")
        db.connect()
        curr_coint_data_max_dates = db._get_max_date_for_coint_pair()
        if curr_coint_data_max_dates is None:
            curr_coint_data_max_dates = pd.DataFrame(columns=["symbol_one", "symbol_two", "date", "window_size", "coint_p_value"])
        db.close()
        
        results = []
        processed_pairs = set()
        for sym1 in group1_symbols:
            print(f"Processing group1 symbol: {sym1}")
            for sym2 in group2_symbols:
                print(f"Processing group2 symbol: {sym2}")
                # Skip if same symbol or if pair already processed
                if sym1 == sym2:
                    print(f"Skipping pair {sym1}-{sym2} as they are the same symbol.")
                    continue
                    
                pair = tuple(sorted([sym1, sym2]))
                if pair in processed_pairs:
                    print(f"Skipping pair {pair} as it has already been processed.")
                    continue
                processed_pairs.add(pair)
                
                print(f"Processing cointegration for pair: {sym1}-{sym2}")
                
                # Get latest date for this symbol pair
                pair_data = curr_coint_data_max_dates[
                    (curr_coint_data_max_dates['symbol_one'] == sym1) & 
                    (curr_coint_data_max_dates['symbol_two'] == sym2)
                ]
                
                # If pair exists in database, only calculate from latest date - window_size
                # If pair doesn't exist, calculate for all dates
                if len(pair_data) > 0:
                    latest_date = pd.to_datetime(pair_data['max_date'].max())
                    start_date = latest_date - pd.Timedelta(days=window_size) 
                    data1 = market_data_pivot[sym1][market_data_pivot.index >= start_date.date()]
                    data2 = market_data_pivot[sym2][market_data_pivot.index >= start_date.date()]
                    print(f"Updating existing pair {sym1}-{sym2} from {latest_date}")
                    print(f"Data1 length: {len(data1)}, Data2 length: {len(data2)}")
                else:
                    # For new pairs, use all available data
                    data1 = market_data_pivot[sym1]
                    data2 = market_data_pivot[sym2]
                    print(f"Processing new pair {sym1}-{sym2} for all available dates")
                    print(f"Data1 length: {len(data1)}, Data2 length: {len(data2)}")
                
                p_values = _single_pair_rolling_coint(
                    data1,
                    data2,
                    window_size
                )
                print(f"Calculated p-values for pair {sym1}-{sym2}: {p_values}")
                
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
                
                print(f"Completed processing pair {sym1}-{sym2}")
        
        if not results:
            return pd.DataFrame()
            
        results_df = pd.DataFrame(results)
        results_df['date'] = pd.to_datetime(results_df['date'])
        return results_df
    
    print("Starting market data transformation")
    
    # Prepare market data transformation
    market_data_pivot = market_data.pivot(index='date', columns='symbol', values='close')
    market_data_pivot = market_data_pivot.sort_index()
    print("Market data transformation completed")
    
    # run
    print("Starting cointegration calculation")
    results_df = calculate_group_cointegration(market_data_pivot, group1_symbols=group1_symbols, group2_symbols=group2_symbols, window_size=window_size)
    print("Cointegration calculation completed")
    
    # update
    print("Starting database update")
    db = binance_cointegration_db_refresher(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD, "binance_analyzer_cointegration")
    db.connect()
    db.create_table()
    db.insert_data(results_df)
    print("Database update completed")
    
if __name__ == "__main__":
    lambda_handler({
    "group1_symbols": ["BTC", "ETH"],
    "group2_symbols": ["LTC", "BNB"]}, {})
    # group1_symbols = event.get('group1_symbols', ["BTC", "ETH", "SOL", "DOGE", "PEPE", "XRP", "BNB", "SHIB", "AVAX", "SUI", "ADA", "NEAR", "FLOKI", "LINK", "FET",
    # "SEI", "OP", "FIL", "FTM", "LTC", "PEOPLE", "INJ", "DOT", "TRX", "APT"]) 

    # group2_symbols = event.get('group2_symbols', [
    # "BTC", "ETH", "SOL", "DOGE", "PEPE", "XRP", "BNB", "SHIB",
    # "AVAX", "SUI", "ARB", "ADA", "WLD", "NEAR", "FLOKI", "RUNE", "LINK", "FET",
    # "SEI", "OP", "FIL", "FTM", "LTC", "PEOPLE", "INJ", "DOT", "TRX", "APT",
    # "BCH", "GALA", "ICP", "UNI", "TRB", "ETC", "STX", "LUNC",
    # "ENS", "XLM", "ARKM", "HBAR", "ATOM", "PENDLE", "DYDX", "AAVE", "JASMY", "LDO",
    # "FTT", "AR", "CRV", "CKB", "LUNA", "OM"
    # ]) 
