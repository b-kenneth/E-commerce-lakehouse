# tests/conftest.py
import pytest
import tempfile
import os
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("LakehouseTests") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    yield spark
    spark.stop()

@pytest.fixture
def temp_dir():
    """Create temporary directory for test files"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir

@pytest.fixture
def sample_csv_data():
    """Sample CSV data for testing"""
    return {
        'orders': "order_num,order_id,user_id,order_timestamp,total_amount,date\nORD_001,1,100,2024-01-01 10:00:00,50.0,2024-01-01",
        'products': "product_id,department_id,department,product_name\n1,1,Electronics,Laptop",
        'order_items': "id,order_id,user_id,days_since_prior_order,product_id,add_to_cart_order,reordered,order_timestamp,date\n1,1,100,5,1,1,0,2024-01-01 10:00:00,2024-01-01"
    }
