# src/glue_jobs/orders_etl.py
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from delta.tables import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3

args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 
    'RAW_BUCKET', 
    'PROCESSED_BUCKET',
    'ENVIRONMENT'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Delta Lake
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

class OrdersETL:
    def __init__(self, raw_bucket, processed_bucket, environment):
        self.raw_bucket = raw_bucket
        self.processed_bucket = processed_bucket
        self.environment = environment
        self.s3_client = boto3.client('s3')
        
    def validate_orders_data(self, df: DataFrame) -> tuple:
        """Validate orders data and return valid/invalid records"""
        
        # Define validation rules based on your data structure
        valid_df = df.filter(
            (col("order_id").isNotNull()) &
            (col("user_id").isNotNull()) &
            (col("total_amount") > 0) &
            (col("order_timestamp").isNotNull()) &
            (col("date").isNotNull()) &
            # Ensure order_id is positive integer
            (col("order_id") > 0) &
            # Ensure total_amount is reasonable (between 0.01 and 10000)
            (col("total_amount").between(0.01, 10000.0))
        )
        
        invalid_df = df.subtract(valid_df)
        return valid_df, invalid_df
    
    def deduplicate_orders(self, df: DataFrame) -> DataFrame:
        """Remove duplicate orders based on order_id, keeping latest by timestamp"""
        window_spec = Window.partitionBy("order_id").orderBy(desc("order_timestamp"))
        return df.withColumn("row_num", row_number().over(window_spec)) \
                 .filter(col("row_num") == 1) \
                 .drop("row_num")
    
    def transform_orders(self, df: DataFrame) -> DataFrame:
        """Apply transformations to orders data"""
        return df.withColumn("order_timestamp", to_timestamp(col("order_timestamp"))) \
                 .withColumn("date", to_date(col("date"))) \
                 .withColumn("processing_timestamp", current_timestamp()) \
                 .withColumn("year", year(col("date"))) \
                 .withColumn("month", month(col("date"))) \
                 .withColumn("day", dayofmonth(col("date"))) \
                 .withColumn("hour", hour(col("order_timestamp")))
    
    def write_to_delta(self, df: DataFrame, table_path: str):
        """Write DataFrame to Delta Lake with merge logic"""
        
        if DeltaTable.isDeltaTable(spark, table_path):
            # Merge logic for existing table
            delta_table = DeltaTable.forPath(spark, table_path)
            
            delta_table.alias("target").merge(
                df.alias("source"),
                "target.order_id = source.order_id"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
        else:
            # Initial write with partitioning
            df.write.format("delta") \
              .mode("overwrite") \
              .partitionBy("year", "month") \
              .save(table_path)
    
    def log_rejected_records(self, invalid_df: DataFrame, reason: str):
        """Log rejected records to S3 for audit"""
        if invalid_df.count() > 0:
            rejected_path = f"s3://{self.processed_bucket}/rejected/orders/{reason}/"
            invalid_df.withColumn("rejection_reason", lit(reason)) \
                     .withColumn("rejection_timestamp", current_timestamp()) \
                     .write.mode("append").parquet(rejected_path)
    
    def process_orders_excel(self, file_path: str):
        """Process Excel files with multiple sheets"""
        
        # Read all sheets from Excel file
        # Note: In production, you'd convert Excel to CSV first via Lambda
        raw_df = spark.read.option("header", "true") \
                          .option("inferSchema", "true") \
                          .csv(file_path)
        
        print(f"Processing {raw_df.count()} raw orders records from {file_path}")
        
        # Validate data
        valid_df, invalid_df = self.validate_orders_data(raw_df)
        
        # Log rejected records
        if invalid_df.count() > 0:
            print(f"Found {invalid_df.count()} invalid records")
            self.log_rejected_records(invalid_df, "validation_failed")
        
        # Deduplicate
        deduped_df = self.deduplicate_orders(valid_df)
        print(f"After deduplication: {deduped_df.count()} records")
        
        # Transform
        transformed_df = self.transform_orders(deduped_df)
        
        # Write to Delta Lake
        delta_path = f"s3://{self.processed_bucket}/lakehouse-dwh/orders/"
        self.write_to_delta(transformed_df, delta_path)
        
        print(f"Successfully processed orders to {delta_path}")
        return transformed_df.count()

# Main execution
if __name__ == "__main__":
    etl = OrdersETL(
        raw_bucket=args['RAW_BUCKET'],
        processed_bucket=args['PROCESSED_BUCKET'],
        environment=args['ENVIRONMENT']
    )
    
    # Process all CSV files in orders directory
    input_path = f"s3://{args['RAW_BUCKET']}/orders/*.csv"
    processed_count = etl.process_orders_excel(input_path)
    
    print(f"Orders ETL job completed. Processed {processed_count} records.")

job.commit()
