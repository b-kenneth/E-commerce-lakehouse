# src/lambda/lambda_function.py
import json
import boto3
import urllib.parse
import os
import pandas as pd
from io import BytesIO

def lambda_handler(event, context):
    """
    S3 Event trigger that converts files and starts Step Functions execution
    - CSV files: Move to processing folder
    - Excel files: Convert all sheets to CSV and move to processing folder
    """
    
    stepfunctions = boto3.client('stepfunctions')
    s3_client = boto3.client('s3')
    
    # Get Step Function ARN from environment variable
    step_function_arn = os.environ.get('STEP_FUNCTION_ARN')
    
    if not step_function_arn:
        error_msg = "STEP_FUNCTION_ARN environment variable not set"
        print(f"❌ {error_msg}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_msg})
        }
    
    print(f"🔧 Using Step Function ARN: {step_function_arn}")
    
    try:
        print(f"📥 Received S3 event with {len(event.get('Records', []))} record(s)")
        
        processed_files = 0
        
        # Parse S3 event
        for i, record in enumerate(event.get('Records', [])):
            print(f"🔍 Processing record {i+1}:")
            
            bucket = record['s3']['bucket']['name']
            key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
            event_name = record['eventName']
            event_time = record['eventTime']
            
            print(f"   📁 Bucket: {bucket}")
            print(f"   📄 Key: {key}")
            print(f"   🎯 Event: {event_name}")
            print(f"   ⏰ Time: {event_time}")
            
            # Check if file is in incoming folder
            if not key.startswith('incoming/'):
                print(f"⏭️ Skipping file not in incoming folder: {key}")
                continue
            
            # Skip directories
            if key.endswith('/'):
                print(f"⏭️ Skipping directory: {key}")
                continue
            
            # Only process data files
            valid_extensions = ['.csv', '.xlsx', '.xls']
            if not any(key.lower().endswith(ext) for ext in valid_extensions):
                print(f"⏭️ Skipping non-data file: {key}")
                continue
            
            print(f"✅ Processing data file: {key}")
            
            # Determine dataset type
            dataset_type = 'unknown'
            if 'product' in key.lower():
                dataset_type = 'products'
            elif 'order' in key.lower():
                if 'item' in key.lower():
                    dataset_type = 'order_items'
                else:
                    dataset_type = 'orders'
            
            print(f"📊 Detected dataset type: {dataset_type}")
            
            # Convert and move files to processing folder
            try:
                converted_files = convert_and_move_file(s3_client, bucket, key, dataset_type)
                print(f"✅ Converted {len(converted_files)} file(s)")
                
                # Start Step Functions execution
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
                
                print(f"🚀 Starting Step Functions execution:")
                print(f"   📋 Name: {execution_name}")
                print(f"   📊 Dataset: {dataset_type}")
                print(f"   📄 Converted files: {len(converted_files)}")
                
                response = stepfunctions.start_execution(
                    stateMachineArn=step_function_arn,
                    name=execution_name,
                    input=json.dumps(execution_input)
                )
                
                print(f"✅ Step Functions execution started successfully!")
                print(f"   🔗 Execution ARN: {response['executionArn']}")
                processed_files += 1
                
            except stepfunctions.exceptions.ExecutionAlreadyExists:
                print(f"⚠️ Execution with name {execution_name} already exists - skipping")
                processed_files += 1
            except Exception as conversion_error:
                print(f"❌ Failed to convert/process file {key}: {str(conversion_error)}")
                continue
            
        print(f"📊 Summary: Processed {processed_files} file(s) successfully")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Files converted and Step Functions executions started for {processed_files} files',
                'processed_files': processed_files,
                'total_records': len(event.get('Records', [])),
                'step_function_arn': step_function_arn
            })
        }
        
    except Exception as e:
        error_msg = f"Error processing S3 event: {str(e)}"
        print(f"💥 {error_msg}")
        
        import traceback
        print(f"🔍 Full error traceback:")
        print(traceback.format_exc())
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': error_msg,
                'step_function_arn': step_function_arn if 'step_function_arn' in locals() else 'Not set'
            })
        }

def convert_and_move_file(s3_client, bucket, key, dataset_type):
    """Convert Excel to CSV or move CSV to processing folder"""
    
    converted_files = []
    
    try:
        if key.lower().endswith('.csv'):
            # Move CSV file to processing folder
            processing_key = key.replace('incoming/', 'processing/')
            
            s3_client.copy_object(
                CopySource={'Bucket': bucket, 'Key': key},
                Bucket=bucket,
                Key=processing_key
            )
            
            converted_files.append(processing_key)
            print(f"✅ Moved CSV: {key} -> {processing_key}")
            
        elif key.lower().endswith(('.xlsx', '.xls')):
            # Download Excel file
            response = s3_client.get_object(Bucket=bucket, Key=key)
            excel_content = response['Body'].read()
            
            # Read Excel file with pandas
            excel_file = pd.read_excel(BytesIO(excel_content), sheet_name=None, engine='openpyxl')
            
            print(f"📊 Found {len(excel_file)} sheet(s) in Excel file")
            
            # Convert each sheet to CSV
            for sheet_name, df in excel_file.items():
                if df.empty:
                    print(f"⚠️ Skipping empty sheet: {sheet_name}")
                    continue
                
                # Generate CSV filename
                base_name = key.split('/')[-1].replace('.xlsx', '').replace('.xls', '')
                csv_key = f"processing/{dataset_type}/{base_name}_{sheet_name}.csv"
                
                # Convert DataFrame to CSV
                csv_buffer = df.to_csv(index=False)
                
                # Upload CSV to S3
                s3_client.put_object(
                    Bucket=bucket,
                    Key=csv_key,
                    Body=csv_buffer,
                    ContentType='text/csv'
                )
                
                converted_files.append(csv_key)
                print(f"✅ Converted sheet '{sheet_name}' to: {csv_key} ({len(df)} records)")
        
        return converted_files
        
    except Exception as e:
        print(f"💥 Error converting file {key}: {str(e)}")
        raise e

def create_execution_name(dataset_type, key):
    """Create unique execution name"""
    import time
    timestamp = str(int(time.time()))
    file_name = key.split('/')[-1].split('.')[0]
    execution_name = f"lakehouse-etl-{dataset_type}-{file_name}-{timestamp}"
    return execution_name.replace('_', '-').replace(' ', '-')[:80]
