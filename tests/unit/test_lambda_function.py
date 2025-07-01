# tests/unit/test_lambda_functions.py
import pytest
import json
from unittest.mock import Mock, patch
from moto import mock_s3, mock_stepfunctions
import boto3

# Import your Lambda functions
from src.lambda.lambda_function import lambda_handler, detect_file_format, determine_dataset_type
from src.lambda.file_processor import validate_dataset_files, list_csv_files

class TestS3TriggerLambda:
    
    @pytest.fixture
    def sample_s3_event(self):
        return {
            "Records": [{
                "s3": {
                    "bucket": {"name": "lakehouse-raw-dev"},
                    "object": {"key": "incoming/orders/test_orders.csv", "size": 1024}
                },
                "eventName": "ObjectCreated:Put",
                "eventTime": "2024-01-15T10:00:00Z"
            }]
        }
    
    def test_detect_file_format_csv(self):
        """Test CSV file format detection"""
        assert detect_file_format("test.csv") == "csv"
        assert detect_file_format("TEST.CSV") == "csv"
    
    def test_detect_file_format_excel(self):
        """Test Excel file format detection"""
        assert detect_file_format("test.xlsx") == "excel"
        assert detect_file_format("test.xls") == "excel"
    
    def test_detect_file_format_unsupported(self):
        """Test unsupported file format"""
        with pytest.raises(ValueError):
            detect_file_format("test.txt")
    
    def test_determine_dataset_type(self):
        """Test dataset type determination"""
        assert determine_dataset_type("incoming/orders/test.csv") == "orders"
        assert determine_dataset_type("incoming/products/test.csv") == "products"
        assert determine_dataset_type("incoming/order_items/test.csv") == "order_items"
        assert determine_dataset_type("incoming/unknown/test.csv") is None
    
    @mock_stepfunctions
    @patch.dict('os.environ', {'STEP_FUNCTION_ARN': 'arn:aws:states:us-east-1:123456789012:stateMachine:test'})
    def test_lambda_handler_success(self, sample_s3_event):
        """Test successful Lambda execution"""
        # Mock Step Functions
        client = boto3.client('stepfunctions', region_name='us-east-1')
        client.create_state_machine(
            name='test',
            definition=json.dumps({"Comment": "Test"}),
            roleArn='arn:aws:iam::123456789012:role/test'
        )
        
        response = lambda_handler(sample_s3_event, {})
        
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['processed_files'] == 1

class TestFileProcessorLambda:
    
    @mock_s3
    def test_list_csv_files(self):
        """Test CSV file listing functionality"""
        # Setup mock S3
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        
        # Upload test files
        s3_client.put_object(Bucket='test-bucket', Key='processing/orders/file1.csv', Body='test')
        s3_client.put_object(Bucket='test-bucket', Key='processing/orders/file2.csv', Body='test')
        s3_client.put_object(Bucket='test-bucket', Key='processing/orders/readme.txt', Body='test')
        
        files = list_csv_files(s3_client, 'test-bucket', 'processing/orders/')
        
        assert len(files) == 2
        assert 'processing/orders/file1.csv' in files
        assert 'processing/orders/file2.csv' in files
        assert 'processing/orders/readme.txt' not in files
    
    @mock_s3
    def test_validate_dataset_files_success(self):
        """Test successful dataset validation"""
        # Setup mock S3 with valid CSV
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        
        csv_content = "order_num,order_id,user_id,order_timestamp,total_amount,date\nORD_001,1,100,2024-01-01 10:00:00,50.0,2024-01-01"
        s3_client.put_object(Bucket='test-bucket', Key='processing/orders/test.csv', Body=csv_content)
        
        result = validate_dataset_files(s3_client, 'test-bucket', 'processing/orders/', 'orders')
        
        assert result['is_valid'] is True
        assert len(result['files_validated']) == 1
        assert len(result['errors']) == 0
    
    @mock_s3
    def test_validate_dataset_files_missing_headers(self):
        """Test validation with missing headers"""
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        
        csv_content = "order_num,user_id,total_amount\nORD_001,100,50.0"  # Missing required headers
        s3_client.put_object(Bucket='test-bucket', Key='processing/orders/test.csv', Body=csv_content)
        
        result = validate_dataset_files(s3_client, 'test-bucket', 'processing/orders/', 'orders')
        
        assert result['is_valid'] is False
        assert len(result['errors']) > 0
        assert 'missing headers' in result['errors'][0].lower()
