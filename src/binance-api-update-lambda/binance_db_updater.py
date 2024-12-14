from config import *
from dotenv import load_dotenv
import os
from helper_functions import binance_ohlc_api_getter
from binance.client import Client
import boto3

def lambda_handler(event, context):
        # Load environment variables and initialize clients
        load_dotenv(override=True)  
        s3_client = boto3.client('s3')
        bucket_name = "lambda-use-zoyu"
        
 

        # Upload to S3
        s3_file_path = f'{json_save_path}/{interval.split("_")[-1]}/{symbol}USDT.json'
        s3_key = f'{symbol}.json'
        
        # insert to DB
        # OHLC
        db = coin_gecko_OHLC_db_refresher(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD, "coin_historical_price")
        db.connect()
        db.create_table()
        for filename in os.listdir(GECKO_DAILY_JSON_PATH):
            if filename.endswith('.json'):
                file_path = os.path.join(GECKO_DAILY_JSON_PATH, filename)
                db.insert_data(file_path)
        db.close()
        
        # insert coin overview from the overview file
        db = coin_gecko_overview_db_refresher(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD, "coin_overview")
        db.connect()
        db.create_table()
        db.insert_data(GECKO_JSON_PATH+'/mapping/top_symbol_by_mc.json')
        db.close()