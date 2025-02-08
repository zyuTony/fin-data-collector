from dotenv import load_dotenv
import os
from db_helper_functions import binance_performance_db_refresher
import logging

def lambda_handler(event, context):
        try:
            # Load environment variables and initialize clients
            load_dotenv(override=True)  
            DB_NAME = os.getenv('DB_NAME')
            DB_HOST = os.getenv('DB_HOST')
            DB_USERNAME = os.getenv('DB_USERNAME')
            DB_PASSWORD = os.getenv('DB_PASSWORD')
            # Connect to database
            db = binance_performance_db_refresher(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD, "binance_periods_performance")
            db.connect()
            
            db.delete_table()
            db.create_table()
            
            db.insert_data()
            db.close()
            
            return {
                'statusCode': 200,
                'body': 'Successfully updated database'
            }
        except Exception as e:
            return {
                'statusCode': 500,
                'body': f'Error updating database: {str(e)}'
            }

if __name__ == "__main__":
    lambda_handler(None, None)
