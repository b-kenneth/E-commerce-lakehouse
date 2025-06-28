import json
import boto3
import pandas as pd
from io import BytesIO
import awswrangler as wr

def lambda_handler(event, context):
    """
    Multi-format file processor and validator
    Handles CSV and Excel files with pre-processing validation
    """
    
    s3_client = boto3.client('s3')
    
    # Extract S3 event information
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
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
            processed_files = process_excel_file(bucket, key)
        else:
            processed_files = process_csv_file(bucket, key)
        
        # Step 4: Trigger appropriate Glue jobs
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
    """Validate file structure and headers before processing"""
    
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
        
        # Read file headers based on format
        if file_format == 'excel':
            headers = get_excel_headers(bucket, key)
        else:
            headers = get_csv_headers(bucket, key)
        
        # Validate headers against expected schema
        expected_headers = expected_schemas[dataset_type]
        missing_headers = set(expected_headers) - set(headers)
        
        if missing_headers:
            validation_result['errors'].append(f"Missing required headers: {list(missing_headers)}")
            validation_result['is_valid'] = False
        
        # Additional validations
        if len(headers) == 0:
            validation_result['errors'].append("No headers found in file")
            validation_result['is_valid'] = False
            
        print(f"✅ Header validation completed for {dataset_type}")
        print(f"📋 Expected: {expected_headers}")
        print(f"📋 Found: {headers}")
        
    except Exception as e:
        validation_result['errors'].append(f"Validation error: {str(e)}")
        validation_result['is_valid'] = False
    
    return validation_result

def get_excel_headers(bucket, key):
    """Extract headers from Excel file (first sheet)"""
    try:
        # Read Excel file using awswrangler
        df = wr.s3.read_excel(f's3://{bucket}/{key}', engine='openpyxl', nrows=1)
        return list(df.columns)
    except Exception as e:
        print(f"Error reading Excel headers: {str(e)}")
        return []

def get_csv_headers(bucket, key):
    """Extract headers from CSV file"""
    try:
        # Read only first row to get headers
        df = wr.s3.read_csv(f's3://{bucket}/{key}', nrows=1)
        return list(df.columns)
    except Exception as e:
        print(f"Error reading CSV headers: {str(e)}")
        return []

def process_excel_file(bucket, key):
    """Process Excel file with multiple sheets"""
    
    print(f"📊 Processing Excel file: {key}")
    processed_files = []
    
    try:
        # Read all sheets from Excel file
        excel_file = wr.s3.read_excel(f's3://{bucket}/{key}', engine='openpyxl', sheet_name=None)
        
        for sheet_name, df in excel_file.items():
            if df.empty:
                continue
                
            # Generate CSV file name based on sheet
            csv_key = key.replace('.xlsx', f'_{sheet_name}.csv').replace('.xls', f'_{sheet_name}.csv')
            csv_key = csv_key.replace('incoming/', 'processing/')
            
            # Write CSV to processing folder
            wr.s3.to_csv(df, f's3://{bucket}/{csv_key}', index=False)
            
            processed_files.append(csv_key)
            print(f"✅ Converted sheet '{sheet_name}' to {csv_key}")
        
        return processed_files
        
    except Exception as e:
        print(f"Error processing Excel file: {str(e)}")
        raise e

def process_csv_file(bucket, key):
    """Process CSV file (move to processing folder)"""
    
    print(f"📄 Processing CSV file: {key}")
    
    try:
        # Move CSV to processing folder
        processing_key = key.replace('incoming/', 'processing/')
        
        s3_client = boto3.client('s3')
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': key},
            Key=processing_key
        )
        
        return [processing_key]
        
    except Exception as e:
        print(f"Error processing CSV file: {str(e)}")
        raise e

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
