# src/lambda/file_processor.py
import json
import boto3
import csv
from io import StringIO
import urllib.parse

def lambda_handler(event, context):
    """
    Multi-format file processor and validator
    Validates all CSV files in a dataset folder after conversion
    """
    
    s3_client = boto3.client('s3')
    
    # Handle both S3 event and Step Functions input
    if 'Records' in event:
        # S3 Event structure
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    else:
        # Step Functions structure
        bucket = event['bucket']
        key = event['key']
    
    print(f"🔍 Processing file: s3://{bucket}/{key}")
    
    try:
        # Step 1: Determine file format and dataset type
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
        
        # Step 2: Process and convert files to processing folder
        if file_format == 'excel':
            processed_files = copy_to_processing(s3_client, bucket, key)
            print("⚠️ Excel file copied to processing - validation will occur after conversion")
        else:
            processed_files = copy_to_processing(s3_client, bucket, key)
        
        # Step 3: Validate all CSV files in the dataset folder
        processing_prefix = f"processing/{dataset_type}/"
        validation_result = validate_dataset_files(s3_client, bucket, processing_prefix, dataset_type)
        
        if not validation_result['is_valid']:
            print(f"❌ Validation failed: {validation_result['errors']}")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'status': 'validation_failed',
                    'errors': validation_result['errors'],
                    'files_validated': validation_result.get('files_validated', []),
                    'file': key
                })
            }
        
        # Step 4: Determine appropriate Glue jobs
        glue_jobs = determine_glue_jobs(dataset_type)
        
        print(f"✅ Validation passed for {len(validation_result['files_validated'])} files")
        
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
        print(f"💥 Error processing file: {str(e)}")
        import traceback
        print(f"🔍 Full traceback: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'processing_failed',
                'error': str(e),
                'file': key
            })
        }

def detect_file_format(file_key):
    """Detect if file is CSV or Excel"""
    if file_key.lower().endswith(('.xlsx', '.xls')):
        return 'excel'
    elif file_key.lower().endswith('.csv'):
        return 'csv'
    else:
        raise ValueError(f"Unsupported file format: {file_key}")

def determine_dataset_type(key):
    """Determine dataset type from file path"""
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
    """Copy file to processing folder"""
    dataset_type = determine_dataset_type(key)
    processing_key = f"processing/{dataset_type}/{key.split('/')[-1]}"
    
    copy_source = {'Bucket': bucket, 'Key': key}
    s3_client.copy_object(
        CopySource=copy_source,
        Bucket=bucket,
        Key=processing_key
    )
    
    print(f"✅ Copied {key} to {processing_key}")
    return [processing_key]

def list_csv_files(s3_client, bucket, prefix):
    """List all CSV files under a given S3 prefix"""
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
        files = []
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.lower().endswith('.csv') and not key.endswith('/'):
                        files.append(key)
        
        print(f"📁 Found {len(files)} CSV files in {prefix}")
        return files
        
    except Exception as e:
        print(f"Error listing files in {prefix}: {str(e)}")
        return []

def get_csv_headers(s3_client, bucket, key):
    """Get headers of a CSV file from S3"""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key, Range='bytes=0-1024')
        content = response['Body'].read().decode('utf-8')
        csv_reader = csv.reader(StringIO(content))
        headers = next(csv_reader)
        return [h.strip() for h in headers]
    except Exception as e:
        print(f"Error reading headers from {key}: {str(e)}")
        return []

def validate_dataset_files(s3_client, bucket, prefix, dataset_type):
    """Validate all CSV files under prefix against expected schema"""
    
    # Expected schemas for each dataset
    expected_schemas = {
        'orders': ['order_num', 'order_id', 'user_id', 'order_timestamp', 'total_amount', 'date'],
        'products': ['product_id', 'department_id', 'department', 'product_name'],
        'order_items': ['id', 'order_id', 'user_id', 'days_since_prior_order', 'product_id', 'add_to_cart_order', 'reordered', 'order_timestamp', 'date']
    }
    
    print(f"🔍 Validating {dataset_type} files in {prefix}")
    
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
        print(f"📄 Validating file: {file_key}")
        headers = get_csv_headers(s3_client, bucket, file_key)
        
        if not headers:
            errors.append(f'File {file_key}: Could not read headers')
            continue
        
        # Check for missing required headers
        missing = set(expected_headers) - set(headers)
        if missing:
            errors.append(f'File {file_key} missing headers: {list(missing)}')
        else:
            valid_files.append(file_key)
            print(f"✅ File {file_key} validation passed")
    
    is_valid = len(errors) == 0 and len(valid_files) > 0
    
    print(f"📊 Validation summary: {len(valid_files)} valid files, {len(errors)} errors")
    
    return {
        'is_valid': is_valid, 
        'errors': errors, 
        'files_validated': valid_files,
        'expected_headers': expected_headers,
        'total_files_checked': len(files)
    }

def determine_glue_jobs(dataset_type):
    """Determine which Glue jobs to trigger based on dataset type"""
    
    job_mapping = {
        'orders': ['lakehouse-orders-etl'],
        'products': ['lakehouse-products-etl'],
        'order_items': ['lakehouse-order-items-etl']
    }
    
    return job_mapping.get(dataset_type, [])
