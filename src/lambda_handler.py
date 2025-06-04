import json
import boto3
import os
from src.logger import get_logger
from src.db_operations import process_files
from src.s3_processor import update_checkpoint, get_files_to_process, get_checkpoint
from dotenv import load_dotenv
load_dotenv()

logger = get_logger(__name__)

# Create a S3 client
s3_client = boto3.client('s3')

secret_name = os.environ['SECRET_NAME']
region_name = os.environ['REGION_NAME']

logger.info("secret_name = '" + secret_name + "'")
logger.info("region_name = '" + region_name + "'")

# Create a Secrets Manager client
session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name=region_name
)

logger.info("Boto session created")

get_secret_value_response = client.get_secret_value(SecretId=secret_name)

secret = get_secret_value_response['SecretString']
jsonSecret = json.loads(secret)


logger.info("SSM secret loaded")

def lambda_handler(event, context):
    """
    Main Lambda handler function
    """
    try:
        
        S3_BUCKET = os.environ['S3_BUCKET'] 
        S3_PREFIX = os.environ['S3_PREFIX']
        CHECKPOINT_KEY = os.environ['CHECKPOINT_KEY']
        MISSING_DATES_KEY = os.environ['MISSING_DATES_KEY']
        
        if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            logger.info("Running inside AWS Lambda")
            DB_CONFIG = {
                'dbname': jsonSecret['DB_NAME'],
                'user': jsonSecret['DB_USER'],
                'password': jsonSecret['DB_PASS'],
                'host': jsonSecret['DB_HOST'],
                'port': jsonSecret['DB_PORT']
            }
        else:
            logger.info("Running Locally")
            DB_CONFIG = {
                'dbname': os.environ['DB_NAME'],
                'user': os.environ['DB_USER'],
                'password': os.environ['DB_PASSWORD'],
                'host': os.environ['DB_HOST'],
                'port': os.environ['DB_PORT']
            }
        
        # Get the last processed date (checkpoint)
        
        # For Testing Only
        # last_processed_date = os.environ['DEFAULT_DATE']
        
        last_processed_date =  get_checkpoint(s3_client, S3_BUCKET, CHECKPOINT_KEY)
        logger.info(f"Last processed date: {last_processed_date}")
        
        # Get list of files to process
        files_to_process = get_files_to_process(s3_client,S3_BUCKET, S3_PREFIX, last_processed_date, MISSING_DATES_KEY)
        
        if not files_to_process:
            logger.info("No new files to process")
            return {
                'statusCode': 200,
                'body': json.dumps('No new files to process')
            }
        
        logger.info(f"Found {len(files_to_process)} files to process")
        
        # Process files and insert into database
        processed_files = process_files(s3_client,S3_BUCKET, files_to_process, DB_CONFIG)
        
        # Update checkpoint with the latest processed date
        if processed_files:
            latest_date = max(processed_files)
            update_checkpoint(s3_client,S3_BUCKET, CHECKPOINT_KEY, latest_date)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Successfully processed {len(processed_files)} files',
                'processed_dates': processed_files
            })
        }
        
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")
        raise


