import os
from io import StringIO
from datetime import datetime, timedelta
import pandas as pd
from src.logger import get_logger
import json

logger = get_logger(__name__)

def get_files_to_process(s3_client, bucket, prefix, last_processed_date, missing_dates_key):
    """
    Scan from checkpoint to today + check any missing dates
    """
    files_to_process = []
    
    # Get missing dates list
    missing_dates = get_missing_dates(s3_client, bucket, missing_dates_key)
    logger.info(f"Missing dates: {missing_dates}")
    
    # Scan from checkpoint to today
    start_date = datetime.strptime(last_processed_date, '%Y_%m_%d') + timedelta(days=1)
    logger.info(f"Start date: {start_date}")
    end_date = datetime.now()
    
    # Collect all dates to check (consecutive + missing)
    dates_to_check = []
    
    # Add consecutive dates
    current_date = start_date
    while current_date <= end_date:
        dates_to_check.append(current_date.strftime('%Y_%m_%d'))
        current_date += timedelta(days=1)
    
    # Add missing dates (only recent ones)
    for missing_date in missing_dates:
        if missing_date not in dates_to_check:
            dates_to_check.append(missing_date)
            
    logger.info(f"Dates to check: {dates_to_check}")
    
    # Check each date for files
    for date_str in dates_to_check:
        date_prefix = f"{prefix}consumption_{date_str}/"
        logger.info(f"Checking prefix: {date_prefix}")
        
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=date_prefix,
            MaxKeys=10
        )
        
        file_found = False
        
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                logger.info(f"Key: {key}")
                
                if key.endswith('.csv') and f'consumption_{date_str}' in key:
                    logger.info(f"Found file: {key}")
                    files_to_process.append({
                        'key': key,
                        'date': date_str,
                        'size': obj['Size'],
                        'last_modified': obj['LastModified']
                    })
                    file_found = True
        
        if not file_found and date_str not in missing_dates:                
            # If file doesn't exist, add to missing dates
            logger.info(f"No file found for date {date_str}, adding to missing dates")
            missing_dates.append(date_str)
    
    
    # Update missing dates file
    update_missing_dates(s3_client, bucket, missing_dates, files_to_process, missing_dates_key)
    
    files_to_process.sort(key=lambda x: x['date'])
    
    return files_to_process

def read_csv_from_s3(s3_client, bucket, key):
    """
    Read CSV file from S3 and return as pandas DataFrame
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        csv_content = response['Body'].read().decode('utf-8')
        
        # Read CSV into DataFrame
        df = pd.read_csv(StringIO(csv_content))
        
        # Convert date column if it exists
        if 'date' in df.columns:
            # Handle different date formats
            try:
                # Try the format from your example first
                df['date'] = pd.to_datetime(df['date'], format='%d-%b-%y').dt.date
            except:
                try:
                    # Try other common formats
                    df['date'] = pd.to_datetime(df['date']).dt.date
                except:
                    logger.warning(f"Could not parse date column in {key}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error reading CSV from S3 {key}: {str(e)}")
        return None


def update_checkpoint(s3_client, bucket, checkpoint_key, date_value):
    """
    Update the checkpoint file in S3
    """
    try:
        
        # Get current checkpoint
        current_checkpoint = get_checkpoint(s3_client, bucket, checkpoint_key)
       
        # Compare dates - only update if new date is greater
        if date_value <= current_checkpoint:
           logger.info(f"Skipping checkpoint update. Current: {current_checkpoint}, New: {date_value}")
           return
       
        # Create checkpoint content with metadata
        checkpoint_content = {
            'last_processed_date': date_value,
            'updated_at': datetime.now().isoformat(),
            'processor': 'data-ingestion'
        }
        
        # Write just the date as plain text for simplicity
        s3_client.put_object(
            Bucket=bucket,
            Key=checkpoint_key,
            Body=date_value,
            ContentType='text/plain',
            Metadata={
                'updated_at': datetime.now().isoformat(),
                'processor': 'data-ingestion'
            }
        )
        logger.info(f"Checkpoint updated to: {date_value}")
        
        # Also create a detailed JSON checkpoint for audit purposes
        json_checkpoint_key = checkpoint_key.replace('.txt', '_detailed.json')
        s3_client.put_object(
            Bucket=bucket,
            Key=json_checkpoint_key,
            Body=json.dumps(checkpoint_content, indent=2),
            ContentType='application/json'
        )
        
    except Exception as e:
        logger.error(f"Error updating checkpoint: {str(e)}")
        raise
    
    

def get_missing_dates(s3_client, bucket, missing_dates_key):
    """Get list of missing dates from S3"""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=missing_dates_key)
        missing_dates = response['Body'].read().decode('utf-8').strip().split('\n')
        return [date for date in missing_dates if date]  # Remove empty lines
    except s3_client.exceptions.NoSuchKey:
        return []

def update_missing_dates(s3_client, bucket, missing_dates, processed_files, missing_dates_key):
    """Update missing dates file, removing processed ones"""
    processed_dates = [f['date'] for f in processed_files]
    
    # Remove processed dates from missing list
    updated_missing = [date for date in missing_dates if date not in processed_dates]
    
    # Remove dates older than 30 days
    cutoff_date = (datetime.now() - timedelta(days=30)).strftime('%Y_%m_%d')
    updated_missing = [date for date in updated_missing if date > cutoff_date]
    
    # Save updated missing dates
    missing_content = '\n'.join(updated_missing)
    s3_client.put_object(
        Bucket=bucket,
        Key=missing_dates_key,
        Body=missing_content,
        ContentType='text/plain'
    )
    
    
def get_checkpoint(s3_client, bucket, checkpoint_key):
    """
    Get the last processed date from S3 checkpoint file
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=checkpoint_key)
        checkpoint_date = response['Body'].read().decode('utf-8').strip()
        logger.info(f"Found checkpoint: {checkpoint_date}")
        return checkpoint_date
    except s3_client.exceptions.NoSuchKey:
         # If checkpoint file doesn't exist, start from a default date
        default_date = os.environ['DEFAULT_DATE'] 
        logger.info(f"Checkpoint file not found, starting from: {default_date}")
        return default_date
    except Exception as e:
        logger.error(f"Error getting checkpoint: {str(e)}")
        # Return default date on any error
        default_date = os.environ['DEFAULT_DATE'] 
        return default_date