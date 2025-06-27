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
from pyspark.sql.window import Window
import boto3
from datetime import datetime

# Get job arguments
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 
    'RAW_BUCKET', 
    'PROCESSED_BUCKET',
    'ENVIRONMENT',
    'GLUE_DATABASE'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Delta Lake
for key, value in {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.databricks.delta.retentionDurationCheck.enabled": "false"
}.items():
    spark.conf.set(key, value)

class OrdersETL:
    def __init__(self, raw_bucket, processed_bucket, environment, glue_database):
        self.raw_bucket = raw_bucket
        self.processed_bucket = processed_bucket
        self.environment = environment
        self.glue_database = glue_database
        self.s3_client = boto3.client('s3')
        
    def validate_orders_data(self, df: DataFrame) -> tuple:
        """Validate orders data according to project requirements"""
        
        print("🔍 Applying validation rules...")
        
        # Define validation rules
        valid_df = df.filter(
            # No null primary identifiers
            (col("order_id").isNotNull()) &
            (col("order_num").isNotNull()) &
            (col("user_id").isNotNull()) &
            
            # Valid timestamps
            (col("order_timestamp").isNotNull()) &
            (to_timestamp(col("order_timestamp")).isNotNull()) &
            
            # Business logic validations
            (col("total_amount") > 0) &
            (col("total_amount") <= 50000.0) &  # Reasonable upper limit
            (col("user_id") > 0) &
            (col("order_id") > 0)
        )
        
        invalid_df = df.subtract(valid_df)
        
        print(f"✅ Valid records: {valid_df.count()}")
        print(f"❌ Invalid records: {invalid_df.count()}")
        
        return valid_df, invalid_df
    
    def deduplicate_orders(self, df: DataFrame) -> DataFrame:
        """Remove duplicate orders based on order_id, keeping latest by timestamp"""
        
        print("🔄 Deduplicating orders...")
        
        initial_count = df.count()
        
        # Use window function to keep latest record per order_id
        window_spec = Window.partitionBy("order_id").orderBy(desc("order_timestamp"))
        deduped_df = df.withColumn("row_num", row_number().over(window_spec)) \
                      .filter(col("row_num") == 1) \
                      .drop("row_num")
        
        final_count = deduped_df.count()
        duplicates_removed = initial_count - final_count
        
        print(f"📊 Removed {duplicates_removed} duplicate records")
        print(f"📊 Final record count: {final_count}")
        
        return deduped_df
    
    def transform_orders(self, df: DataFrame) -> DataFrame:
        """Apply transformations to orders data"""
        
        print("🔧 Applying transformations...")
        
        return df.withColumn("order_timestamp", to_timestamp(col("order_timestamp"))) \
                 .withColumn("date", to_date(col("date"))) \
                 .withColumn("processing_timestamp", current_timestamp()) \
                 .withColumn("year", year(col("date"))) \
                 .withColumn("month", month(col("date"))) \
                 .withColumn("day", dayofmonth(col("date"))) \
                 .withColumn("hour", hour(col("order_timestamp"))) \
                 .withColumn("order_value_category", 
                    when(col("total_amount") < 50, "Low")
                    .when(col("total_amount") < 200, "Medium")
                    .otherwise("High"))
    
    def write_to_delta_lake(self, df: DataFrame, table_path: str):
        """Write DataFrame to Delta Lake with ACID compliance"""
        
        print(f"💾 Writing to Delta Lake: {table_path}")
        
        if DeltaTable.isDeltaTable(spark, table_path):
            print("📝 Performing MERGE operation (upsert)")
            
            delta_table = DeltaTable.forPath(spark, table_path)
            
            # Merge logic with ACID compliance
            delta_table.alias("target").merge(
                df.alias("source"),
                "target.order_id = source.order_id"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
             
            print("✅ MERGE operation completed")
        else:
            print("📝 Initial write with partitioning")
            
            # Initial write with partitioning for performance
            df.write.format("delta") \
              .mode("overwrite") \
              .partitionBy("year", "month") \
              .option("path", table_path) \
              .saveAsTable(f"{self.glue_database}.orders")
              
            print("✅ Initial Delta table created")
    
    def log_rejected_records(self, invalid_df: DataFrame, reason: str):
        """Log rejected records with detailed audit trail"""
        
        if invalid_df.count() > 0:
            print(f"📝 Logging {invalid_df.count()} rejected records")
            
            rejected_path = f"s3://{self.processed_bucket}/rejected/orders/{reason}/"
            
            invalid_df.withColumn("rejection_reason", lit(reason)) \
                     .withColumn("rejection_timestamp", current_timestamp()) \
                     .withColumn("job_run_id", lit(args['JOB_NAME'])) \
                     .write.mode("append") \
                     .option("path", rejected_path) \
                     .parquet(rejected_path)
                     
            print(f"✅ Rejected records logged to: {rejected_path}")
    
    def archive_processed_files(self, source_path: str):
        """Archive successfully processed files"""
        
        print("📦 Archiving processed files...")
        
        try:
            # List files in incoming directory
            response = self.s3_client.list_objects_v2(
                Bucket=self.raw_bucket,
                Prefix='incoming/orders/'
            )
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].endswith('.csv'):
                        # Copy to archived folder
                        copy_source = {'Bucket': self.raw_bucket, 'Key': obj['Key']}
                        archive_key = obj['Key'].replace('incoming/', 'archived/')
                        
                        self.s3_client.copy_object(
                            CopySource=copy_source,
                            Bucket=self.raw_bucket,
                            Key=archive_key
                        )
                        
                        # Delete from incoming
                        self.s3_client.delete_object(
                            Bucket=self.raw_bucket,
                            Key=obj['Key']
                        )
                        
                        print(f"✅ Archived: {obj['Key']} -> {archive_key}")
                        
        except Exception as e:
            print(f"⚠️ Archive operation failed: {str(e)}")
    
    def process_orders(self):
        """Main processing logic for orders"""
        
        print("🚀 Starting Orders ETL Process")
        print(f"📍 Environment: {self.environment}")
        print(f"📍 Raw Bucket: {self.raw_bucket}")
        print(f"📍 Processed Bucket: {self.processed_bucket}")
        
        try:
            # Read raw data from incoming folder
            input_path = f"s3://{self.raw_bucket}/incoming/orders/"
            print(f"📖 Reading data from: {input_path}")
            
            raw_df = spark.read.option("header", "true") \
                              .option("inferSchema", "true") \
                              .csv(input_path)
            
            initial_count = raw_df.count()
            print(f"📊 Initial record count: {initial_count}")
            
            if initial_count == 0:
                print("⚠️ No data found to process")
                return 0
            
            # Step 1: Validate data
            valid_df, invalid_df = self.validate_orders_data(raw_df)
            
            # Step 2: Log rejected records
            if invalid_df.count() > 0:
                self.log_rejected_records(invalid_df, "validation_failed")
            
            # Step 3: Deduplicate
            deduped_df = self.deduplicate_orders(valid_df)
            
            # Step 4: Transform
            transformed_df = self.transform_orders(deduped_df)
            
            # Step 5: Write to Delta Lake
            delta_path = f"s3://{self.processed_bucket}/lakehouse-dwh/orders/"
            self.write_to_delta_lake(transformed_df, delta_path)
            
            # Step 6: Archive processed files
            self.archive_processed_files(input_path)
            
            final_count = transformed_df.count()
            print(f"🎉 Orders ETL completed successfully!")
            print(f"📊 Final processed records: {final_count}")
            
            return final_count
            
        except Exception as e:
            print(f"💥 Error in Orders ETL: {str(e)}")
            raise e

# Main execution
if __name__ == "__main__":
    etl = OrdersETL(
        raw_bucket=args['RAW_BUCKET'],
        processed_bucket=args['PROCESSED_BUCKET'],
        environment=args['ENVIRONMENT'],
        glue_database=args['GLUE_DATABASE']
    )
    
    processed_count = etl.process_orders()
    print(f"🏁 Orders ETL job completed. Processed {processed_count} records.")

job.commit()
