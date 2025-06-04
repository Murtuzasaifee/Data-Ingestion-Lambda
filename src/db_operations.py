import psycopg2
import pandas as pd
from src.logger import get_logger
from src.s3_processor import read_csv_from_s3

logger = get_logger(__name__)

def process_files(s3_client, bucket, files_to_process, db_config):
    """
    Process CSV files and insert data into PostgreSQL
    """
    processed_dates = []
    
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        for file_info in files_to_process:
            try:
                logger.info(f"Processing file: {file_info['key']}")
                
                # Read CSV from S3
                df = read_csv_from_s3(s3_client,bucket, file_info['key'])
                
                if df is not None and not df.empty:
                    # Process and insert data
                    rows_inserted, is_success = insert_data_to_postgres(cursor, df)
                    conn.commit()
                    
                    if is_success:
                        logger.info(f"Successfully processed {file_info['key']}: {rows_inserted} rows inserted")
                        processed_dates.append(file_info['date'])
                    else:
                        logger.info(f"Row insertion failed: {file_info['key']}")
                else:
                    logger.warning(f"No data found in file: {file_info['key']}")
                    
            except Exception as e:
                logger.error(f"Error processing file {file_info['key']}: {str(e)}")
                conn.rollback()
                # Exit the process if the insertion fails
                break
        
        cursor.close()
        
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()
    
    return processed_dates

def insert_data_to_postgres(cursor, df):
    """
    Insert DataFrame data into PostgreSQL consumptions table
    """
    rows_inserted = 0
    is_success = False
    
    try:
        # Expected columns based on your schema
        expected_columns = ['date', 'client_id', 'client_name', 'service_name', 'total_consumed_tokens']
        
        # Validate columns exist
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            logger.warning(f"Missing columns: {missing_columns}")
            is_success = False
            return 0
        
        # Insert data row by row
        for index, row in df.iterrows():
            try:
                cursor.execute('''
                    INSERT INTO consumptions (date, client_id, client_name, service_name, total_consumed_tokens, created_at, updated_at, is_active)
                    VALUES (%s, %s, %s, %s, %s, NOW(), NOW(), TRUE)
                    ON CONFLICT (date, client_id) DO UPDATE SET
                        client_name = EXCLUDED.client_name,
                        service_name = EXCLUDED.service_name,
                        total_consumed_tokens = EXCLUDED.total_consumed_tokens,
                        updated_at = NOW()
                ''', (
                    row['date'],
                    row['client_id'],
                    row['client_name'],
                    row['service_name'],
                    int(row['total_consumed_tokens']) if pd.notna(row['total_consumed_tokens']) else 0
                ))
                rows_inserted += 1
                is_success = True
                
            except Exception as e:
                logger.error(f"Error inserting row {index}: {str(e)}")
                # Exit the process if the insertion fails
                is_success = False
                break
        
        return rows_inserted, is_success
        
    except Exception as e:
        logger.error(f"Error in insert_data_to_postgres: {str(e)}")
        is_success = False
        raise