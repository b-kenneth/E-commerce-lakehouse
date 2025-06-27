import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("test") \
        .master("local[*]") \
        .getOrCreate()

def test_orders_validation_logic(spark):
    """Test orders validation rules"""
    
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("order_num", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("order_timestamp", StringType(), True)
    ])
    
    test_data = [
        (1, "ORD_001", 100, 50.0, "2024-01-01 10:00:00"),  # valid
        (None, "ORD_002", 101, 75.0, "2024-01-01 11:00:00"),  # invalid - null order_id
        (3, "ORD_003", 102, -10.0, "2024-01-01 12:00:00"),  # invalid - negative amount
        (4, "ORD_004", 103, 100.0, None),  # invalid - null timestamp
    ]
    
    df = spark.createDataFrame(test_data, schema)
    
    # Apply validation logic
    valid_df = df.filter(
        (col("order_id").isNotNull()) &
        (col("order_num").isNotNull()) &
        (col("user_id").isNotNull()) &
        (col("total_amount") > 0) &
        (col("order_timestamp").isNotNull()) &
        (to_timestamp(col("order_timestamp")).isNotNull())
    )
    
    assert valid_df.count() == 1
    assert valid_df.collect()[0]["order_id"] == 1

def test_deduplication_logic(spark):
    """Test order deduplication keeps latest record"""
    
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("order_timestamp", StringType(), True),
        StructField("total_amount", DoubleType(), True)
    ])
    
    test_data = [
        (1, "2024-01-01 10:00:00", 50.0),
        (1, "2024-01-01 11:00:00", 55.0),  # later timestamp - should be kept
        (2, "2024-01-01 10:00:00", 75.0),
    ]
    
    df = spark.createDataFrame(test_data, schema)
    
    # Apply deduplication logic
    from pyspark.sql.window import Window
    window_spec = Window.partitionBy("order_id").orderBy(desc("order_timestamp"))
    deduped_df = df.withColumn("row_num", row_number().over(window_spec)) \
                   .filter(col("row_num") == 1) \
                   .drop("row_num")
    
    assert deduped_df.count() == 2
    # Check that the later record for order_id=1 is kept
    order_1_record = deduped_df.filter(col("order_id") == 1).collect()[0]
    assert order_1_record["total_amount"] == 55.0
