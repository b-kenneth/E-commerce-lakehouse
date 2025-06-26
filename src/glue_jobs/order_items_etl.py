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

class OrderItemsETL:
    def __init__(self, raw_bucket, processed_bucket, environment):
        self.raw_bucket = raw_bucket
        self.processed_bucket = processed_bucket
        self.environment = environment
        
    def validate_order_items_data(self, df: DataFrame) -> tuple:
        """Validate order items data"""
        
        valid_df = df.filter(
            (col("id").isNotNull()) &
            (col("order_id").isNotNull()) &
            (col("user_id").isNotNull()) &
            (col("product_id").isNotNull()) &
            (col("add_to_cart_order").isNotNull()) &
            (col("reordered").isin([0, 1])) &
            # Ensure positive IDs
            (col("id") > 0) &
            (col("order_id") > 0) &
            (col("product_id") > 0) &
            # Reasonable cart order (1-50)
            (col("add_to_cart_order").between(1, 50))
        )
        
        invalid_df = df.subtract(valid_df)
        return valid_df, invalid_df
    
    def deduplicate_order_items(self, df: DataFrame) -> DataFrame:
        """Remove duplicate order items based on id"""
        return df.dropDuplicates(["id"])
    
    def transform_order_items(self, df: DataFrame) -> DataFrame:
        """Apply transformations to order items data"""
        return df.withColumn("order_timestamp", to_timestamp(col("order_timestamp"))) \
                 .withColumn("date", to_date(col("date"))) \
                 .withColumn("processing_timestamp", current_timestamp()) \
                 .withColumn("year", year(col("date"))) \
                 .withColumn("month", month(col("date"))) \
                 .withColumn("is_reorder", col("reordered").cast("boolean"))
    
    def write_to_delta(self, df: DataFrame, table_path: str):
        """Write DataFrame to Delta Lake with merge logic"""
        
        if DeltaTable.isDeltaTable(spark, table_path):
            delta_table = DeltaTable.forPath(spark, table_path)
            
            delta_table.alias("target").merge(
                df.alias("source"),
                "target.id = source.id"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
        else:
            df.write.format("delta") \
              .mode("overwrite") \
              .partitionBy("year", "month") \
              .save(table_path)
    
    def process_order_items(self, file_path: str):
        """Main processing logic for order items"""
        
        raw_df = spark.read.option("header", "true") \
                          .option("inferSchema", "true") \
                          .csv(file_path)
        
        print(f"Processing {raw_df.count()} raw order items records")
        
        # Validate data
        valid_df, invalid_df = self.validate_order_items_data(raw_df)
        
        if invalid_df.count() > 0:
            print(f"Found {invalid_df.count()} invalid records")
            # Log rejected records
            rejected_path = f"s3://{self.processed_bucket}/rejected/order_items/"
            invalid_df.withColumn("rejection_reason", lit("validation_failed")) \
                     .withColumn("rejection_timestamp", current_timestamp()) \
                     .write.mode("append").parquet(rejected_path)
        
        # Deduplicate
        deduped_df = self.deduplicate_order_items(valid_df)
        print(f"After deduplication: {deduped_df.count()} records")
        
        # Transform
        transformed_df = self.transform_order_items(deduped_df)
        
        # Write to Delta Lake
        delta_path = f"s3://{self.processed_bucket}/lakehouse-dwh/order_items/"
        self.write_to_delta(transformed_df, delta_path)
        
        print(f"Successfully processed order items to {delta_path}")
        return transformed_df.count()

# Main execution
if __name__ == "__main__":
    etl = OrderItemsETL(
        raw_bucket=args['RAW_BUCKET'],
        processed_bucket=args['PROCESSED_BUCKET'],
        environment=args['ENVIRONMENT']
    )
    
    input_path = f"s3://{args['RAW_BUCKET']}/order_items/*.csv"
    processed_count = etl.process_order_items(input_path)
    
    print(f"Order Items ETL job completed. Processed {processed_count} records.")

job.commit()
