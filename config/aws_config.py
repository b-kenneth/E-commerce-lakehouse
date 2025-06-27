import os

class AWSConfig:
    """Production-grade AWS configuration for the lakehouse project"""
    
    # S3 Bucket Configuration
    RAW_BUCKET = "lakehouse-raw-dev"
    PROCESSED_BUCKET = "lakehouse-processed-dev"
    
    # S3 Path Structure
    PATHS = {
        # Raw zone paths
        'raw_incoming_orders': f"{RAW_BUCKET}/incoming/orders/",
        'raw_incoming_products': f"{RAW_BUCKET}/incoming/products/",
        'raw_incoming_order_items': f"{RAW_BUCKET}/incoming/order_items/",
        'raw_processing': f"{RAW_BUCKET}/processing/",
        'raw_archived': f"{RAW_BUCKET}/archived/",
        'raw_scripts': f"{RAW_BUCKET}/scripts/",
        
        # Processed zone paths (Delta Lake)
        'processed_lakehouse': f"{PROCESSED_BUCKET}/lakehouse-dwh/",
        'processed_orders': f"{PROCESSED_BUCKET}/lakehouse-dwh/orders/",
        'processed_products': f"{PROCESSED_BUCKET}/lakehouse-dwh/products/",
        'processed_order_items': f"{PROCESSED_BUCKET}/lakehouse-dwh/order_items/",
        'processed_rejected': f"{PROCESSED_BUCKET}/rejected/",
    }
    
    # AWS Settings
    AWS_REGION = "us-east-1"
    ENVIRONMENT = "dev"
    
    # Glue Job Configuration
    GLUE_JOBS = {
        'orders_etl': {
            'name': 'lakehouse-orders-etl',
            'script_location': f"s3://{RAW_BUCKET}/scripts/glue-jobs/orders_etl.py",
            'role': 'lakehouse-glue-service-role',
            'max_capacity': 2,
            'timeout': 60
        },
        'products_etl': {
            'name': 'lakehouse-products-etl',
            'script_location': f"s3://{RAW_BUCKET}/scripts/glue-jobs/products_etl.py",
            'role': 'lakehouse-glue-service-role',
            'max_capacity': 2,
            'timeout': 30
        },
        'order_items_etl': {
            'name': 'lakehouse-order-items-etl',
            'script_location': f"s3://{RAW_BUCKET}/scripts/glue-jobs/order_items_etl.py",
            'role': 'lakehouse-glue-service-role',
            'max_capacity': 2,
            'timeout': 60
        }
    }
    
    # Step Functions Configuration
    STEP_FUNCTION_NAME = "lakehouse-etl-orchestrator"
    
    # Glue Data Catalog
    GLUE_DATABASE = "lakehouse_ecommerce_db"
    
    # Delta Lake Configuration
    DELTA_LAKE_CONFIG = {
        'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
        'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
        'spark.databricks.delta.retentionDurationCheck.enabled': 'false',
        'spark.databricks.delta.merge.repartitionBeforeWrite.enabled': 'true'
    }
    
    @classmethod
    def get_glue_job_args(cls, job_type):
        """Get common arguments for Glue jobs"""
        return {
            '--RAW_BUCKET': cls.RAW_BUCKET,
            '--PROCESSED_BUCKET': cls.PROCESSED_BUCKET,
            '--ENVIRONMENT': cls.ENVIRONMENT,
            '--GLUE_DATABASE': cls.GLUE_DATABASE,
            '--enable-metrics': '',
            '--enable-continuous-cloudwatch-log': 'true',
            '--enable-spark-ui': 'true',
            '--spark-event-logs-path': f's3://{cls.RAW_BUCKET}/spark-logs/',
            '--job-language': 'python',
            '--additional-python-modules': 'delta-spark==3.0.0'
        }
    
    @classmethod
    def get_s3_path(cls, path_key):
        """Get S3 path with s3:// prefix"""
        return f"s3://{cls.PATHS[path_key]}"
