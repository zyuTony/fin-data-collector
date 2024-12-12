from config import *
from dotenv import load_dotenv
import os
from data_getter import binance_ohlc_api_getter
from binance.client import Client

load_dotenv(override=True)    
bn_api_key = os.environ['BINANCE_API']
bn_api_secret = os.environ['BINANCE_SECRET'] 

def lambda_handler(event, context):
    try:
        # Load environment variables
        bn_api_key = os.environ['BINANCE_API']
        bn_api_secret = os.environ['BINANCE_SECRET']

        end_date = '2024-04-01' 
        start_date = '2024-01-01'
        json_save_path = '/tmp'  # Lambda can only write to /tmp directory

        api_getter = binance_ohlc_api_getter(
            api_key=bn_api_key, 
            api_secret=bn_api_secret,
            data_save_path=json_save_path,
            interval='1d',
            start_date=start_date,
            end_date=end_date
        )
        
        result = api_getter._download_single_symbol('ADA')
        print(result)
        return {
            'statusCode': 200,
            'body': result
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }
 
 