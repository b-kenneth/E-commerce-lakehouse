# src/lambda/s3_event_trigger.py
import json
import boto3
import urllib.parse
import os

def lambda_handler(event, context):
    """
    S3 Event trigger that starts Step Functions execution
    Triggered when files are uploaded to incoming/ folders
    """
    
    stepfunctions = boto3.client('stepfunctions')
    
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
            
            # Only process files in incoming folders
            if '/incoming/' not in key:
                print(f"⏭️ Skipping file not in incoming folder: {key}")
                continue
            
            # Skip directories and non-data files
            if key.endswith('/') or not any(key.lower().endswith(ext) for ext in ['.csv', '.xlsx', '.xls']):
                print(f"⏭️ Skipping non-data file: {key}")
                continue
            
            print(f"✅ Processing data file: {key}")
            
            # Create execution name (must be unique and valid)
            import time
            timestamp = str(int(time.time()))
            file_name = key.split('/')[-1].split('.')[0]  # Get filename without extension
            execution_name = f"lakehouse-etl-{file_name}-{timestamp}"
            
            # Ensure execution name is valid (Step Functions naming rules)
            execution_name = execution_name.replace('_', '-').replace(' ', '-')[:80]
            
            # Start Step Functions execution
            execution_input = {
                'bucket': bucket,
                'key': key,
                'timestamp': event_time,
                'event_name': event_name,
                'file_type': key.split('.')[-1].lower(),
                'dataset_type': 'orders' if 'order' in key.lower() and 'item' not in key.lower() 
                              else 'order_items' if 'order' in key.lower() and 'item' in key.lower()
                              else 'products' if 'product' in key.lower() 
                              else 'unknown'
            }
            
            print(f"🚀 Starting Step Functions execution:")
            print(f"   📋 Name: {execution_name}")
            print(f"   🎯 ARN: {step_function_arn}")
            print(f"   📊 Input: {json.dumps(execution_input, indent=2)}")
            
            response = stepfunctions.start_execution(
                stateMachineArn=step_function_arn,
                name=execution_name,
                input=json.dumps(execution_input)
            )
            
            print(f"✅ Step Functions execution started successfully!")
            print(f"   🔗 Execution ARN: {response['executionArn']}")
            
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Step Functions executions started successfully',
                'executions': len(event.get('Records', [])),
                'step_function_arn': step_function_arn
            })
        }
        
    except Exception as e:
        error_msg = f"Error processing S3 event: {str(e)}"
        print(f"💥 {error_msg}")
        
        # Print full error details for debugging
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
