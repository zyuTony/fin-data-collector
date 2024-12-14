from config import *
from dotenv import load_dotenv
import os
from binance.client import Client
from datetime import datetime, timedelta

def lambda_handler(event, context):
        # Load environment variables and initialize clients
        load_dotenv(override=True)   
        bn_api_key = os.environ.get('BINANCE_API')
        bn_api_secret = os.environ.get('BINANCE_SECRET')
        
        # return all traded tickers
        client = Client(api_key=bn_api_key, api_secret=bn_api_secret)
        tickers = client.get_all_tickers()

        # Extract symbols ending with USDT
        symbols = []
        for ticker in tickers:
            symbol = ticker['symbol']
            if symbol.endswith('USDT'):
                base_symbol = symbol[:-4]  # Remove 'USDT' from the end
                symbols.append(base_symbol)

        # Set default dates if not provided
        today = datetime.today().strftime('%Y-%m-%d')
        five_days_ago = (datetime.today() - timedelta(days=5)).strftime('%Y-%m-%d')
        
        end_date = event.get('end_date', today)
        start_date = event.get('start_date', five_days_ago)
        interval = event.get('interval', '1d')
        
        result = {'start_date': start_date,
                  'end_date': end_date,
                  'interval': interval,
                  'symbols': symbols}
        return result

# Test section
if __name__ == "__main__":
    # Test case 1: With provided dates and interval
    test_event_1 = {
        "start_date": "2024-01-01",
        "end_date": "2024-04-01", 
        "interval": "1d"
    }
    result_1 = lambda_handler(test_event_1, None)
    print("Test 1 - With provided parameters:")
    print(result_1)

    # Test case 2: Without any parameters (using defaults)
    test_event_2 = {}
    result_2 = lambda_handler(test_event_2, None)
    print("\nTest 2 - With default parameters:")
    print(result_2)
