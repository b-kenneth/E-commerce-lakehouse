# src/lambda/file_processor.py
import json
import boto3
import csv
from io import StringIO
import urllib.parse
import logging

# Configure structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Multi-format file processor and validator
    Validates all CSV files in a dataset folder after conversion
    """
    
    s3_client = boto3.client('s3')
    logger.info(f"Received event: {json.dumps(event, indent=2)}")
    
    try:
        # Extract bucket and key from event
        bucket = None
        key = None

        if 'bucket' in event and 'key' in event:
            bucket = event['bucket']
            key = event['key']
            logger.info("Found bucket and key at top level")

        elif 'lambda_result' in event and 'Payload' in event['lambda_result']:
            payload = event['lambda_result']['Payload']
            if isinstance(payload, str):
                payload = json.loads(payload)
            bucket = payload.get('bucket')
            key = payload.get('key')
            logger.info("Found bucket and key in lambda_result.Payload")

        elif 'Records' in event and len(event['Records']) > 0:
            record = event['Records'][0]
            bucket = record['s3']['bucket']['name']
            key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
            logger.info("Found bucket and key in S3 Records")

        elif 'original_key' in event:
            bucket = event.get('bucket')
            key = event.get('original_key') or event.get('key')
            logger.info("Found bucket and key with original_key")

        if not bucket or not key:
            error_msg = f"Could not extract bucket and key from event. Available keys: {list(event.keys())}"
            logger.error(error_msg)
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'status': 'validation_failed',
                    'errors': [error_msg],
                    'event_keys': list(event.keys())
                })
            }

        logger.info(f"Processing file: s3://{bucket}/{key}")
        
        file_format = detect_file_format(key)
        dataset_type = determine_dataset_type(key)
        
        if not dataset_type:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'status': 'validation_failed',
                    'errors': ['Cannot determine dataset type from file path'],
                    'file': key
                })
            }

        logger.info(f"Detected dataset type: {dataset_type}, format: {file_format}")

        processed_files = copy_to_processing(s3_client, bucket, key)

        if file_format == 'excel':
            logger.info("Excel file copied to processing - validation will occur after conversion")

        prefix = f"processing/{dataset_type}/"
        validation_result = validate_dataset_files(s3_client, bucket, prefix, dataset_type)

        if not validation_result['is_valid']:
            logger.warning(f"Validation failed: {validation_result['errors']}")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'status': 'validation_failed',
                    'errors': validation_result['errors'],
                    'files_validated': validation_result.get('files_validated', []),
                    'file': key
                })
            }

        glue_jobs = determine_glue_jobs(dataset_type)
        logger.info(f"Validation passed for {len(validation_result['files_validated'])} files")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'ready_for_processing',
                'processed_files': validation_result['files_validated'],
                'glue_jobs': glue_jobs,
                'validation_passed': True,
                'dataset_type': dataset_type
            })
        }

    except Exception as e:
        logger.error(f"Error processing file: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'processing_failed',
                'error': str(e),
                'event_keys': list(event.keys()) if isinstance(event, dict) else 'event_not_dict'
            })
        }

def detect_file_format(file_key):
    if file_key.lower().endswith(('.xlsx', '.xls')):
        return 'excel'
    elif file_key.lower().endswith('.csv'):
        return 'csv'
    else:
        raise ValueError(f"Unsupported file format: {file_key}")

def determine_dataset_type(key):
    key_lower = key.lower()
    if 'product' in key_lower:
        return 'products'
    elif 'order' in key_lower:
        if 'item' in key_lower:
            return 'order_items'
        else:
            return 'orders'
    return None

def copy_to_processing(s3_client, bucket, key):
    dataset_type = determine_dataset_type(key)
    processing_key = f"processing/{dataset_type}/{key.split('/')[-1]}"
    s3_client.copy_object(
        CopySource={'Bucket': bucket, 'Key': key},
        Bucket=bucket,
        Key=processing_key
    )
    logger.info(f"Copied {key} to {processing_key}")
    return [processing_key]

def list_csv_files(s3_client, bucket, prefix):
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        files = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.lower().endswith('.csv') and not key.endswith('/'):
                        files.append(key)
        logger.info(f"Found {len(files)} CSV files in {prefix}")
        return files
    except Exception as e:
        logger.error(f"Error listing files in {prefix}: {str(e)}")
        return []

def get_csv_headers(s3_client, bucket, key):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key, Range='bytes=0-1024')
        content = response['Body'].read().decode('utf-8')
        csv_reader = csv.reader(StringIO(content))
        headers = next(csv_reader)
        return [h.strip() for h in headers]
    except Exception as e:
        logger.error(f"Error reading headers from {key}: {str(e)}")
        return []

def validate_dataset_files(s3_client, bucket, prefix, dataset_type):
    expected_schemas = {
        'orders': ['order_num', 'order_id', 'user_id', 'order_timestamp', 'total_amount', 'date'],
        'products': ['product_id', 'department_id', 'department', 'product_name'],
        'order_items': ['id', 'order_id', 'user_id', 'days_since_prior_order', 'product_id',
                        'add_to_cart_order', 'reordered', 'order_timestamp', 'date']
    }

    logger.info(f"Validating {dataset_type} files in {prefix}")

    files = list_csv_files(s3_client, bucket, prefix)
    if not files:
        return {
            'is_valid': False,
            'errors': [f'No CSV files found under {prefix}'],
            'files_validated': []
        }

    expected_headers = expected_schemas.get(dataset_type, [])
    if not expected_headers:
        return {
            'is_valid': False,
            'errors': [f'Unknown dataset type: {dataset_type}'],
            'files_validated': files
        }

    errors = []
    valid_files = []

    for file_key in files:
        logger.info(f"Validating file: {file_key}")
        headers = get_csv_headers(s3_client, bucket, file_key)
        if not headers:
            errors.append(f'File {file_key}: Could not read headers')
            continue

        missing = set(expected_headers) - set(headers)
        if missing:
            errors.append(f'File {file_key} missing headers: {list(missing)}')
        else:
            valid_files.append(file_key)
            logger.info(f"File {file_key} validation passed")

    is_valid = len(errors) == 0 and len(valid_files) > 0

    logger.info(f"Validation summary: {len(valid_files)} valid files, {len(errors)} errors")

    return {
        'is_valid': is_valid,
        'errors': errors,
        'files_validated': valid_files,
        'expected_headers': expected_headers,
        'total_files_checked': len(files)
    }

def determine_glue_jobs(dataset_type):
    return {
        'orders': ['lakehouse-orders-etl'],
        'products': ['lakehouse-products-etl'],
        'order_items': ['lakehouse-order-items-etl']
    }.get(dataset_type, [])
