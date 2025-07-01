# Lakehouse E-commerce Data Pipeline

## One-Line Description

An event-driven data lakehouse pipeline for e-commerce analytics using AWS services, Delta Lake, and automated CI/CD orchestration.

## Project Overview

This project implements a comprehensive, scalable, data lakehouse pipeline designed to handle real-world e-commerce transaction data at scale. Built with AWS cloud-native services, it addresses critical challenges in modern data engineering including multi-format data ingestion, automated data quality validation, ACID-compliant storage, and intelligent orchestration.

The pipeline automatically processes diverse data formats (CSV, Excel with multiple sheets), ensures data integrity through comprehensive validation and deduplication, stores data in Delta Lake format for ACID compliance, and provides automated monitoring and alerting. It's designed to handle production workloads with features like schema evolution, error handling, automated archival, and cost optimization.

## Features

- **Multi-Format Data Ingestion**: Automatic processing of CSV and Excel files (with multiple sheets)
- **Event-Driven Architecture**: S3 event triggers with Lambda-based file conversion and validation
- **ACID-Compliant Storage**: Delta Lake implementation with merge/upsert operations and schema evolution
- **Intelligent Data Validation**: Schema enforcement, data type validation, and business rule checks
- **Automated Deduplication**: Advanced deduplication logic preserving latest records by timestamp
- **Production-Grade Orchestration**: AWS Step Functions with comprehensive error handling and retry mechanisms
- **Automated Data Archival**: Successful file processing with organized archival system
- **Real-Time Monitoring**: CloudWatch integration with custom metrics
- **Cost Optimization**: Intelligent resource allocation and S3 lifecycle policies
- **CI/CD Pipeline**: GitHub Actions with automated testing, validation, and deployment
- **Data Catalog Integration**: Automatic schema discovery and Athena query capabilities
- **Comprehensive Logging**: Detailed audit trails and rejected record tracking

## Architecture
![arhitecture diagram](images/architecture_diagram.svg)

**Data Flow Architecture:**

1. **Ingestion Layer**: S3 event-driven file detection and Lambda-based conversion
2. **Processing Layer**: AWS Glue ETL jobs with PySpark and Delta Lake
3. **Storage Layer**: S3-based Delta Lake with partitioning and optimization
4. **Orchestration Layer**: Step Functions with intelligent workflow management
5. **Catalog Layer**: Glue Data Catalog with Athena query capabilities
6. **Monitoring Layer**: CloudWatch metrics with SNS alerting

## Tech Stack

**Backend & Data Processing:**
- AWS Glue (PySpark 3.5, Python 3.9)
- Delta Lake 3.0 for ACID-compliant storage
- AWS Lambda (Python 3.9) for event processing
- Pandas & OpenPyXL for file format conversion

**Orchestration & Workflow:**
- AWS Step Functions for pipeline orchestration
- AWS S3 for data lake storage (Raw & Processed zones)
- AWS Glue Crawler for automated schema discovery

**Query & Analytics:**
- Amazon Athena for SQL-based data validation and querying
- AWS Glue Data Catalog for metadata management

**Monitoring & Alerting:**
- AWS CloudWatch for metrics, logs, and dashboards
- AWS SNS for real-time notifications

**CI/CD & Infrastructure:**
- GitHub Actions for automated testing and deployment
- AWS IAM for security and access management
- AWS CloudFormation/CDK for infrastructure as code

**Development & Testing:**
- pytest for unit and integration testing
- boto3 for AWS SDK integration

## Setup Instructions

### Prerequisites
- AWS CLI configured with appropriate permissions
- Python 3.9+
- Git

### 1. Clone the Repository

```bash
git clone https://github.com/b-kenneth/Airflow-glue-pipeline.git
cd lakehouse-ecommerce-pipeline
```

### 2. Install Dependencies

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Configure Environment Variables

Create a `.env` file or export variables:

```bash
# AWS Configuration
export AWS_REGION=us-east-1
export AWS_ACCOUNT_ID=YOUR_ACCOUNT_ID

# S3 Buckets
export RAW_BUCKET=lakehouse-raw-dev
export PROCESSED_BUCKET=lakehouse-processed-dev

# Step Functions
export STEP_FUNCTION_ARN=arn:aws:states:us-east-1:YOUR_ACCOUNT:stateMachine:lakehouse-etl-orchestrator

# Glue Configuration
export GLUE_DATABASE=lakehouse_ecommerce_db
```

### 4. AWS Infrastructure Setup

```bash
# Create S3 buckets
aws s3 mb s3://lakehouse-raw-dev
aws s3 mb s3://lakehouse-processed-dev

# Create folder structure
aws s3api put-object --bucket lakehouse-raw-dev --key incoming/
aws s3api put-object --bucket lakehouse-raw-dev --key processing/
aws s3api put-object --bucket lakehouse-raw-dev --key archived/
```

### 5. Run Tests Locally

```bash
# Run unit tests
pytest tests/unit/ -v

# Run integration tests
pytest tests/integration/ -v

# Generate test coverage report
pytest --cov=src tests/ --cov-report=html
```

## Data Flow / Pipeline Description

### End-to-End Data Pipeline

1. **Data Ingestion**
   - Files (CSV or Excel) uploaded to `s3://lakehouse-raw-dev/incoming/`
   - S3 event notification triggers Lambda function

2. **File Processing & Conversion**
   - Lambda function detects file format and dataset type
   - Excel files: Converts all sheets to individual CSV files
   - CSV files: Validates headers and moves to processing folder
   - All converted files stored in `s3://lakehouse-raw-dev/processing/`

3. **Pipeline Orchestration**
   - Lambda triggers AWS Step Functions execution
   - Step Functions determines appropriate Glue ETL job based on dataset type
   - Parallel processing for multiple datasets

4. **ETL Processing**
   - Glue ETL jobs read CSV files from processing folder
   - Data validation: Schema enforcement, null checks, business rule validation
   - Deduplication: Advanced logic preserving latest records by timestamp
   - Transformation: Data enrichment, type conversion, partitioning preparation
   - Delta Lake Write: ACID-compliant storage with merge/upsert operations

5. **Schema Discovery & Cataloging**
   - Glue Crawler automatically discovers Delta Lake table schemas
   - Updates Glue Data Catalog for Athena query capabilities

6. **Data Validation**
   - Athena executes validation queries on processed data
   - Verifies record counts and data quality metrics

7. **File Archival**
   - Successfully processed files moved to `s3://lakehouse-raw-dev/archived/`
   - Maintains audit trail with timestamps

8. **Monitoring & Alerting**
   - CloudWatch captures detailed metrics and logs
   - SNS sends success/failure notifications
   - Comprehensive error handling with retry mechanisms

## Usage Guide

### Uploading Data

1. **Prepare your data files** in supported formats:
   - **Orders**: CSV or Excel with columns: `order_num`, `order_id`, `user_id`, `order_timestamp`, `total_amount`, `date`
   - **Products**: CSV or Excel with columns: `product_id`, `department_id`, `department`, `product_name`
   - **Order Items**: CSV or Excel with columns: `id`, `order_id`, `user_id`, `days_since_prior_order`, `product_id`, `add_to_cart_order`, `reordered`, `order_timestamp`, `date`

2. **Upload files to appropriate folders**:
   ```bash
   # Upload orders data
   aws s3 cp orders_data.xlsx s3://lakehouse-raw-dev/incoming/orders/
   
   # Upload products data
   aws s3 cp products_data.csv s3://lakehouse-raw-dev/incoming/products/
   
   # Upload order items data
   aws s3 cp order_items_data.xlsx s3://lakehouse-raw-dev/incoming/order_items/
   ```

### Monitoring Pipeline Execution

1. **Step Functions Console**: Monitor real-time execution status
2. **CloudWatch Logs**: View detailed processing logs
3. **SNS Notifications**: Receive email/SMS alerts for pipeline status

### Querying Processed Data

```sql
-- Query orders data
SELECT 
    order_value_category,
    COUNT(*) as order_count,
    AVG(total_amount) as avg_order_value
FROM lakehouse_ecommerce_db.orders 
GROUP BY order_value_category;

-- Query products by department
SELECT 
    department,
    COUNT(*) as product_count
FROM lakehouse_ecommerce_db.products 
GROUP BY department;

-- Join orders with order items
SELECT 
    o.order_id,
    o.total_amount,
    COUNT(oi.id) as item_count
FROM lakehouse_ecommerce_db.orders o
JOIN lakehouse_ecommerce_db.order_items oi ON o.order_id = oi.order_id
GROUP BY o.order_id, o.total_amount;
```

## Deployment Instructions

### GitHub Actions CI/CD Pipeline

1. **Configure GitHub Secrets**:
   ```
   AWS_ACCESS_KEY_ID: Your AWS access key
   AWS_SECRET_ACCESS_KEY: Your AWS secret key
   AWS_ACCOUNT_ID: Your 12-digit AWS account ID
   ```

2. **Automated Deployment**:
   - Push to `main` branch triggers CI/CD pipeline
   - Runs automated tests
   - Deploys Glue scripts to S3
   - Updates Lambda functions
   - Validates infrastructure

### Manual Deployment Steps

1. **Deploy Lambda Functions**:
   ```bash
   # Package and deploy Lambda functions
   cd src/lambda
   zip -r lambda-deployment.zip .
   aws lambda update-function-code --function-name lakehouse-s3-event-trigger --zip-file fileb://lambda-deployment.zip
   ```

2. **Deploy Glue ETL Jobs**:
   ```bash
   # Upload Glue scripts
   aws s3 cp src/glue_jobs/ s3://lakehouse-raw-dev/scripts/glue-jobs/ --recursive
   
   # Update Glue job definitions
   aws glue update-job --job-name lakehouse-orders-etl --job-update file://glue-job-config.json
   ```

3. **Deploy Step Functions**:
   ```bash
   # Update Step Functions state machine
   aws stepfunctions update-state-machine --state-machine-arn $STEP_FUNCTION_ARN --definition file://step-functions-definition.json
   ```

### Production Environment Setup

1. **Multi-Environment Configuration**:
   - Dev: `lakehouse-raw-dev`, `lakehouse-processed-dev`
   - Staging: `lakehouse-raw-staging`, `lakehouse-processed-staging`
   - Prod: `lakehouse-raw-prod`, `lakehouse-processed-prod`

2. **Security Configuration**:
   - IAM roles with least-privilege access
   - S3 bucket encryption and versioning
   - VPC endpoints for secure data transfer

## Tests

### Test Structure
```
tests/
├── unit/
│   ├── test_orders_validation.py
│   ├── test_data_transformation.py
│   └── test_lambda_functions.py
├── integration/
│   ├── test_end_to_end_pipeline.py
│   └── test_glue_jobs.py
└── conftest.py
```

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test categories
pytest tests/unit/ -v                    # Unit tests only
pytest tests/integration/ -v             # Integration tests only

# Run tests with coverage
pytest --cov=src tests/ --cov-report=html

# Run tests in parallel
pytest tests/ -n auto
```

### Test Coverage

- **Unit Tests**: 85%+ coverage for core business logic
- **Integration Tests**: End-to-end pipeline validation
- **CI Pipeline**: Automated testing on every pull request

### Continuous Integration

GitHub Actions automatically:
- Runs full test suite on pull requests
- Validates Spark job syntax
- Checks code quality with linting
- Generates test coverage reports

## Known Issues / Limitations

### Current Limitations

1. **Excel File Size**: Lambda function has 15-minute timeout for large Excel files
2. **Region Dependency**: Currently optimized for `us-east-1` region
3. **Schema Evolution**: Manual intervention required for major schema changes

### Workarounds

- **Large Files**: Use S3 multipart upload and consider Glue Python Shell jobs for very large Excel files
- **Multi-Region**: Environment variables support different regions
- **Schema Changes**: Implement schema versioning strategy