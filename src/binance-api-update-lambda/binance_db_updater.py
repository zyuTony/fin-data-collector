from config import *
from dotenv import load_dotenv
import os
from db_helper_functions import binance_OHLC_db_refresher
import boto3
import tempfile

def lambda_handler(event, context):
        try:
            # Load environment variables and initialize clients
            load_dotenv(override=True)  
            DB_NAME = os.getenv('DB_NAME')
            DB_HOST = os.getenv('DB_HOST')
            DB_USERNAME = os.getenv('DB_USERNAME')
            DB_PASSWORD = os.getenv('DB_PASSWORD')
            
            s3_client = boto3.client('s3')
            bucket_name = "lambda-use-zoyu"
            
            # Create temp directory to store downloaded files
            with tempfile.TemporaryDirectory() as temp_dir:
                # List all JSON files in the S3 bucket
                response = s3_client.list_objects_v2(Bucket=bucket_name)
           
                # Connect to database
                db = binance_OHLC_db_refresher(DB_NAME, DB_HOST, DB_USERNAME, DB_PASSWORD, "binance_market_data")
                db.connect()
                db.create_table()
                
                # Download and process each file
                for obj in response.get('Contents', []):
                    if obj['Key'].endswith('.json'):
                        # Download file from S3
                        temp_file_path = os.path.join(temp_dir, obj['Key'])
                        s3_client.download_file(bucket_name, obj['Key'], temp_file_path)
                        
                        # Insert data into database
                        db.insert_data(temp_file_path)
                
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
