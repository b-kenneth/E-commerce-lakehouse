# src/lambda/file_archiver.py
import boto3
import json
import logging
from datetime import datetime

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Archive successfully processed files from incoming to archived folder
    """
    
    s3_client = boto3.client('s3')
    
    try:
        # Extract bucket information from Step Functions input
        bucket_name = 'lakehouse-raw-dev'  # Your bucket name
        
        # Archive files from incoming folders
        folders_to_archive = ['incoming/orders/', 'incoming/products/', 'incoming/order_items/']
        archived_files = []
        
        for folder in folders_to_archive:
            try:
                # List files in incoming folder
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=folder
                )
                
                if 'Contents' in response:
                    for obj in response['Contents']:
                        if obj['Key'].endswith(('.csv', '.xlsx', '.xls')):
                            # Generate archive path with timestamp
                            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                            archive_key = obj['Key'].replace('incoming/', f'archived/{timestamp}_')
                            
                            # Copy to archived folder
                            copy_source = {'Bucket': bucket_name, 'Key': obj['Key']}
                            s3_client.copy_object(
                                CopySource=copy_source,
                                Bucket=bucket_name,
                                Key=archive_key
                            )
                            
                            # Delete from incoming folder
                            s3_client.delete_object(
                                Bucket=bucket_name,
                                Key=obj['Key']
                            )
                            
                            archived_files.append({
                                'original': obj['Key'],
                                'archived': archive_key
                            })
                            
                            logger.info(f"Archived: {obj['Key']} -> {archive_key}")
                            
            except Exception as e:
                logger.warning(f"Error archiving folder {folder}: {str(e)}")
                continue
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Successfully archived {len(archived_files)} files',
                'archived_files': archived_files
            })
        }
        
    except Exception as e:
        logger.error(f"Error in file archiver: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }
