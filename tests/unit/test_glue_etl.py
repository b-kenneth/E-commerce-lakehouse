# tests/unit/test_glue_etl.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col
from datetime import datetime

class TestOrdersETL:
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Create Spark session for testing"""
        spark = SparkSession.builder \
            .appName("OrdersETLTest") \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    @pytest.fixture
    def sample_orders_data(self, spark):
        """Create sample orders data for testing"""
        schema = StructType([
            StructField("order_num", StringType(), True),
            StructField("order_id", IntegerType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("order_timestamp", StringType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("date", StringType(), True)
        ])
        
        data = [
            ("ORD_001", 1, 100, "2024-01-01 10:00:00", 150.50, "2024-01-01"),
            ("ORD_002", 2, 101, "2024-01-01 11:00:00", 75.25, "2024-01-01"),
            ("ORD_001", 1, 100, "2024-01-01 10:00:00", 150.50, "2024-01-01"),  # Duplicate
            (None, 3, 102, "2024-01-01 12:00:00", -10.0, "2024-01-01"),  # Invalid
        ]
        
        return spark.createDataFrame(data, schema)
    
    def test_validate_orders_data(self, spark, sample_orders_data):
        """Test orders data validation logic"""
        # Import your validation function
        # from src.glue_jobs.orders_etl import OrdersETL
        
        # Apply validation logic (simplified for testing)
        valid_df = sample_orders_data.filter(
            (col("order_id").isNotNull()) &
            (col("order_num").isNotNull()) &
            (col("user_id").isNotNull()) &
            (col("total_amount") > 0)
        )
        
        invalid_df = sample_orders_data.subtract(valid_df)
        
        assert valid_df.count() == 2  # Two valid records
        assert invalid_df.count() == 2  # One duplicate + one invalid
    
    def test_deduplicate_orders(self, spark, sample_orders_data):
        """Test orders deduplication logic"""
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number, desc
        
        # Apply deduplication logic
        window_spec = Window.partitionBy("order_id").orderBy(desc("order_timestamp"))
        deduped_df = sample_orders_data.withColumn("row_num", row_number().over(window_spec)) \
                                      .filter(col("row_num") == 1) \
                                      .drop("row_num")
        
        assert deduped_df.count() == 3  # Original 4 minus 1 duplicate
        
        # Check that order_id=1 appears only once
        order_1_count = deduped_df.filter(col("order_id") == 1).count()
        assert order_1_count == 1
    
    def test_transform_orders(self, spark, sample_orders_data):
        """Test orders transformation logic"""
        from pyspark.sql.functions import to_timestamp, to_date, current_timestamp, year, month
        
        # Apply transformations
        transformed_df = sample_orders_data.withColumn("order_timestamp", to_timestamp(col("order_timestamp"))) \
                                          .withColumn("date", to_date(col("date"))) \
                                          .withColumn("year", year(col("date"))) \
                                          .withColumn("month", month(col("date")))
        
        # Verify transformations
        assert "year" in transformed_df.columns
        assert "month" in transformed_df.columns
        
        # Check data types
        timestamp_field = [f for f in transformed_df.schema.fields if f.name == "order_timestamp"][0]
        assert "timestamp" in str(timestamp_field.dataType).lower()

class TestProductsETL:
    
    @pytest.fixture(scope="class")
    def spark(self):
        spark = SparkSession.builder \
            .appName("ProductsETLTest") \
            .master("local[*]") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    def test_products_validation(self, spark):
        """Test products data validation"""
        schema = StructType([
            StructField("product_id", IntegerType(), True),
            StructField("department_id", IntegerType(), True),
            StructField("department", StringType(), True),
            StructField("product_name", StringType(), True)
        ])
        
        data = [
            (1, 1, "Electronics", "Laptop"),
            (2, 2, "Books", "Python Guide"),
            (None, 1, "Electronics", "Mouse"),  # Invalid - null product_id
            (4, None, "Books", "Data Science"),  # Invalid - null department_id
        ]
        
        df = spark.createDataFrame(data, schema)
        
        # Apply validation
        valid_df = df.filter(
            (col("product_id").isNotNull()) &
            (col("department_id").isNotNull()) &
            (col("department").isNotNull()) &
            (col("product_name").isNotNull())
        )
        
        assert valid_df.count() == 2
        assert df.count() - valid_df.count() == 2  # Two invalid records
