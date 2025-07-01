# src/lambda/lambda_function.py
import json
import boto3
import urllib.parse
import os
import pandas as pd
import logging
from io import BytesIO

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    S3 Event trigger that converts files and starts Step Functions execution.
    Handles CSV and Excel files in the incoming folder.
    """
    
    stepfunctions = boto3.client('stepfunctions')
    s3_client = boto3.client('s3')
    
    step_function_arn = os.environ.get('STEP_FUNCTION_ARN')
    if not step_function_arn:
        logger.error("STEP_FUNCTION_ARN environment variable not set")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': "STEP_FUNCTION_ARN environment variable not set"})
        }

    logger.info(f"Using Step Function ARN: {step_function_arn}")
    
    try:
        records = event.get('Records', [])
        logger.info(f"Received S3 event with {len(records)} record(s)")
        
        processed_files = 0

        for i, record in enumerate(records):
            logger.info(f"Processing record {i+1}")
            bucket = record['s3']['bucket']['name']
            key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
            event_name = record['eventName']
            event_time = record['eventTime']

            logger.info(f"Bucket: {bucket}")
            logger.info(f"Key: {key}")
            logger.info(f"Event: {event_name}")
            logger.info(f"Time: {event_time}")

            if not key.startswith('incoming/'):
                logger.warning(f"Skipping file not in incoming folder: {key}")
                continue

            if key.endswith('/'):
                logger.warning(f"Skipping directory: {key}")
                continue

            valid_extensions = ['.csv', '.xlsx', '.xls']
            if not any(key.lower().endswith(ext) for ext in valid_extensions):
                logger.warning(f"Skipping non-data file: {key}")
                continue

            logger.info(f"Processing data file: {key}")

            dataset_type = 'unknown'
            if 'product' in key.lower():
                dataset_type = 'products'
            elif 'order' in key.lower():
                if 'item' in key.lower():
                    dataset_type = 'order_items'
                else:
                    dataset_type = 'orders'

            logger.info(f"Detected dataset type: {dataset_type}")

            try:
                converted_files = convert_and_move_file(s3_client, bucket, key, dataset_type)
                logger.info(f"Converted {len(converted_files)} file(s)")

                execution_name = create_execution_name(dataset_type, key)

                execution_input = {
                    'bucket': bucket,
                    'original_key': key,
                    'converted_files': converted_files,
                    'timestamp': event_time,
                    'event_name': event_name,
                    'dataset_type': dataset_type,
                    'execution_trigger': 'S3_EVENT_WITH_CONVERSION'
                }

                logger.info("Starting Step Functions execution")
                logger.info(f"Name: {execution_name}")
                logger.info(f"Dataset: {dataset_type}")
                logger.info(f"Converted files: {len(converted_files)}")

                response = stepfunctions.start_execution(
                    stateMachineArn=step_function_arn,
                    name=execution_name,
                    input=json.dumps(execution_input)
                )

                logger.info("Step Functions execution started successfully")
                logger.info(f"Execution ARN: {response['executionArn']}")
                processed_files += 1

            except stepfunctions.exceptions.ExecutionAlreadyExists:
                logger.warning(f"Execution {execution_name} already exists - skipping")
                processed_files += 1
            except Exception as conversion_error:
                logger.error(f"Failed to convert/process file {key}: {str(conversion_error)}", exc_info=True)
                continue

        logger.info(f"Summary: Processed {processed_files} file(s) successfully")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Files converted and Step Functions executions started for {processed_files} files',
                'processed_files': processed_files,
                'total_records': len(records),
                'step_function_arn': step_function_arn
            })
        }

    except Exception as e:
        logger.error(f"Error processing S3 event: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': f"Error processing S3 event: {str(e)}",
                'step_function_arn': step_function_arn
            })
        }

def convert_and_move_file(s3_client, bucket, key, dataset_type):
    """Convert Excel to CSV or move CSV to processing folder"""
    converted_files = []

    try:
        if key.lower().endswith('.csv'):
            processing_key = key.replace('incoming/', 'processing/')
            s3_client.copy_object(
                CopySource={'Bucket': bucket, 'Key': key},
                Bucket=bucket,
                Key=processing_key
            )
            logger.info(f"Moved CSV: {key} -> {processing_key}")
            converted_files.append(processing_key)

        elif key.lower().endswith(('.xlsx', '.xls')):
            response = s3_client.get_object(Bucket=bucket, Key=key)
            excel_content = response['Body'].read()
            excel_file = pd.read_excel(BytesIO(excel_content), sheet_name=None, engine='openpyxl')

            logger.info(f"Found {len(excel_file)} sheet(s) in Excel file")

            for sheet_name, df in excel_file.items():
                if df.empty:
                    logger.warning(f"Skipping empty sheet: {sheet_name}")
                    continue

                base_name = key.split('/')[-1].replace('.xlsx', '').replace('.xls', '')
                csv_key = f"processing/{dataset_type}/{base_name}_{sheet_name}.csv"

                csv_buffer = df.to_csv(index=False)
                s3_client.put_object(
                    Bucket=bucket,
                    Key=csv_key,
                    Body=csv_buffer,
                    ContentType='text/csv'
                )

                converted_files.append(csv_key)
                logger.info(f"Converted sheet '{sheet_name}' to: {csv_key} ({len(df)} records)")

        return converted_files

    except Exception as e:
        logger.error(f"Error converting file {key}: {str(e)}", exc_info=True)
        raise e

def create_execution_name(dataset_type, key):
    import time
    timestamp = str(int(time.time()))
    file_name = key.split('/')[-1].split('.')[0]
    execution_name = f"lakehouse-etl-{dataset_type}-{file_name}-{timestamp}"
    return execution_name.replace('_', '-').replace(' ', '-')[:80]
