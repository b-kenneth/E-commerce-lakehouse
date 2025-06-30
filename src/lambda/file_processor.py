# src/lambda/file_processor.py
import json
import boto3
import csv
from io import StringIO
import urllib.parse

def lambda_handler(event, context):
    """
    Multi-format file processor and validator
    Handles CSV and Excel files with pre-processing validation
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
        # Step 1: Determine file format
        file_format = detect_file_format(key)
        
        # Step 2: Validate file structure
        validation_result = validate_file_structure(bucket, key, file_format)
        
        if not validation_result['is_valid']:
            print(f"❌ Validation failed: {validation_result['errors']}")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'status': 'validation_failed',
                    'errors': validation_result['errors'],
                    'file': key
                })
            }
        
        # Step 3: Process based on format
        if file_format == 'excel':
            # For Excel files, just copy to processing folder
            # Glue will handle Excel processing with proper libraries
            processed_files = copy_to_processing(bucket, key)
            print("⚠️ Excel file copied to processing - Glue will handle conversion")
        else:
            processed_files = process_csv_file(bucket, key)
        
        # Step 4: Determine appropriate Glue jobs
        glue_jobs = determine_glue_jobs(key)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'ready_for_processing',
                'processed_files': processed_files,
                'glue_jobs': glue_jobs,
                'validation_passed': True
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

def validate_file_structure(bucket, key, file_format):
    """Validate file structure using native Python"""
    
    s3_client = boto3.client('s3')
    validation_result = {'is_valid': True, 'errors': []}
    
    try:
        # Define expected schemas
        expected_schemas = {
            'orders': ['order_num', 'order_id', 'user_id', 'order_timestamp', 'total_amount', 'date'],
            'products': ['product_id', 'department_id', 'department', 'product_name'],
            'order_items': ['id', 'order_id', 'user_id', 'days_since_prior_order', 'product_id', 'add_to_cart_order', 'reordered', 'order_timestamp', 'date']
        }
        
        # Determine dataset type from file path
        dataset_type = None
        for ds_type in expected_schemas.keys():
            if ds_type in key.lower():
                dataset_type = ds_type
                break
        
        if not dataset_type:
            validation_result['errors'].append("Cannot determine dataset type from file path")
            validation_result['is_valid'] = False
            return validation_result
        
        # For CSV files, validate headers
        if file_format == 'csv':
            headers = get_csv_headers_native(bucket, key)
            expected_headers = expected_schemas[dataset_type]
            missing_headers = set(expected_headers) - set(headers)
            
            if missing_headers:
                validation_result['errors'].append(f"Missing required headers: {list(missing_headers)}")
                validation_result['is_valid'] = False
        else:
            # For Excel files, defer validation to Glue
            print("⚠️ Excel validation deferred to Glue processing")
        
        print(f"✅ Header validation completed for {dataset_type}")
        
    except Exception as e:
        validation_result['errors'].append(f"Validation error: {str(e)}")
        validation_result['is_valid'] = False
    
    return validation_result

def get_csv_headers_native(bucket, key):
    """Extract headers from CSV file using native Python"""
    try:
        s3_client = boto3.client('s3')
        
        # Read first few bytes to get headers
        response = s3_client.get_object(Bucket=bucket, Key=key, Range='bytes=0-1024')
        content = response['Body'].read().decode('utf-8')
        
        # Parse CSV headers
        csv_reader = csv.reader(StringIO(content))
        headers = next(csv_reader)
        
        return [header.strip() for header in headers]
        
    except Exception as e:
        print(f"Error reading CSV headers: {str(e)}")
        return []

def copy_to_processing(bucket, key):
    """Copy file to processing folder"""
    s3_client = boto3.client('s3')
    
    processing_key = key.replace('incoming/', 'processing/')
    
    copy_source = {'Bucket': bucket, 'Key': key}
    s3_client.copy_object(
        CopySource=copy_source,
        Bucket=bucket,
        Key=processing_key
    )
    
    print(f"✅ Copied {key} to {processing_key}")
    return [processing_key]

def process_csv_file(bucket, key):
    """Process CSV file (copy to processing folder)"""
    
    print(f"📄 Processing CSV file: {key}")
    return copy_to_processing(bucket, key)

def determine_glue_jobs(file_key):
    """Determine which Glue jobs to trigger based on file path"""
    
    if 'orders' in file_key.lower() and 'order_items' not in file_key.lower():
        return ['lakehouse-orders-etl']
    elif 'products' in file_key.lower():
        return ['lakehouse-products-etl']
    elif 'order_items' in file_key.lower():
        return ['lakehouse-order-items-etl']
    else:
        return []
