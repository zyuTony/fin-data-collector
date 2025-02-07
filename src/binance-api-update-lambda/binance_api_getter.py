from config import *
from dotenv import load_dotenv
import os
from helper_functions import binance_ohlc_api_getter
from binance.client import Client
import boto3

def lambda_handler(event, context):
    try:
        # Validate required event parameters
        required_params = ['start_date', 'end_date', 'interval', 'symbol']
        if not all(param in event for param in required_params):
            return {
                'statusCode': 400,
                'body': f'Missing required parameters. Required: {required_params}'
            }

        # Load environment variables and initialize clients
        load_dotenv(override=True)  
        bn_api_key = os.environ.get('BINANCE_API')
        bn_api_secret = os.environ.get('BINANCE_SECRET')
        if not bn_api_key or not bn_api_secret:
            return {
                'statusCode': 500,
                'body': 'Missing required environment variables'
            }

        s3_client = boto3.client('s3')
        bucket_name = "lambda-use-zoyu"
        
        # Extract parameters from event
        symbol = event['symbol']
        start_date = event['start_date']
        end_date = event['end_date']
        interval = event['interval']
        json_save_path = '/tmp'

        # Initialize API getter and download data
        api_getter = binance_ohlc_api_getter(
            api_key=bn_api_key,
            api_secret=bn_api_secret, 
            data_save_path=json_save_path,
            interval=interval,
            start_date=start_date,
            end_date=end_date
        )
        
        download_result = api_getter._download_single_symbol(symbol)
        if download_result == -1:
            return {
                'statusCode': 500,
                'body': f'Failed to download data for symbol {symbol}'
            }

        # Upload to S3
        s3_file_path = f'{json_save_path}/{interval.split("_")[-1]}/{symbol}USDT.json'
        s3_key = f'{symbol}.json'
        try:
            s3_client.upload_file(s3_file_path, bucket_name, s3_key)
        except Exception as e:
            return {
                'statusCode': 500,
                'body': f'Failed to upload to S3: {str(e)}'
            }

        return {
            'statusCode': 200,
            'body': {
                'message': 'Successfully downloaded and uploaded data',
                'symbol': symbol,
                'bucket': bucket_name,
                'key': s3_key
            }
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'type': type(e).__name__
            }
        }

# Test section
if __name__ == "__main__":
    # Test case 1: With valid parameters
    test_event_1 = { 
        "start_date": "2024-01-01",
        "end_date": "2024-04-01",
        "interval": "1d",
        "symbol": "BTC"
    }
    result_1 = lambda_handler(test_event_1, None)
    print("Test 1 - Valid parameters:")
    print(result_1)

    # Test case 2: Missing parameters
    test_event_2 = {
        "start_date": "2024-01-01",
        "symbol": "ETH"
    }
    result_2 = lambda_handler(test_event_2, None)
    print("\nTest 2 - Missing parameters:")
    print(result_2)

    # Test case 3: Invalid symbol
    test_event_3 = {
        "start_date": "2024-01-01",
        "end_date": "2024-04-01",
        "interval": "1d",
        "symbol": "INVALID"
    }
    result_3 = lambda_handler(test_event_3, None)
    print("\nTest 3 - Invalid symbol:")
    print(result_3)