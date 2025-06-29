# src/lambda/s3_event_trigger.py
import json
import boto3
import urllib.parse

def lambda_handler(event, context):
    """
    S3 Event trigger that starts Step Functions execution
    Triggered when files are uploaded to incoming/ folders
    """
    
    stepfunctions = boto3.client('stepfunctions')
    
    try:
        # Parse S3 event
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
            
            print(f"🔔 New file detected: s3://{bucket}/{key}")
            
            # Only process files in incoming folders
            if '/incoming/' not in key:
                print(f"⏭️ Skipping file not in incoming folder: {key}")
                continue
            
            # Start Step Functions execution
            execution_input = {
                'bucket': bucket,
                'key': key,
                'timestamp': record['eventTime'],
                'event_name': record['eventName']
            }
            
            response = stepfunctions.start_execution(
                stateMachineArn='arn:aws:states:us-east-1:ACCOUNT:stateMachine:lakehouse-etl-orchestrator',
                name=f"lakehouse-etl-{int(context.aws_request_id.replace('-', '')[:10], 16)}",
                input=json.dumps(execution_input)
            )
            
            print(f"✅ Step Functions execution started: {response['executionArn']}")
            
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Step Functions executions started successfully',
                'executions': len(event['Records'])
            })
        }
        
    except Exception as e:
        print(f"💥 Error processing S3 event: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }
