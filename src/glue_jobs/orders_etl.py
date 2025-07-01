# src/glue_jobs/orders_etl.py
import sys
import logging
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
import traceback

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

class OrdersETL:
    def __init__(self, raw_bucket, processed_bucket, environment, glue_database):
        self.raw_bucket = raw_bucket
        self.processed_bucket = processed_bucket
        self.environment = environment
        self.glue_database = glue_database
        self.s3_client = boto3.client('s3')
        
        logger.info("Verifying Delta Lake configuration...")
        try:
            extensions = spark.conf.get("spark.sql.extensions", "Not set")
            catalog = spark.conf.get("spark.sql.catalog.spark_catalog", "Not set")
            logger.info(f"Extensions: {extensions}")
            logger.info(f"Catalog: {catalog}")
        except Exception as e:
            logger.warning(f"Configuration check failed: {str(e)}")
    
    def read_multi_format_data(self, input_paths):
        all_dataframes = []
        
        for input_path in input_paths:
            try:
                logger.info(f"Checking path: {input_path}")
                
                prefix = input_path.replace(f's3://{self.raw_bucket}/', '')
                response = self.s3_client.list_objects_v2(
                    Bucket=self.raw_bucket,
                    Prefix=prefix
                )
                
                if 'Contents' not in response:
                    logger.warning(f"No files found in {input_path}")
                    continue
                
                for obj in response['Contents']:
                    file_key = obj['Key']
                    file_path = f"s3://{self.raw_bucket}/{file_key}"
                    
                    if file_key.endswith('/'):
                        continue
                    
                    logger.info(f"Processing file: {file_key}")
                    
                    if file_key.endswith('.csv'):
                        df = spark.read.option("header", "true") \
                                      .option("inferSchema", "true") \
                                      .csv(file_path)
                        all_dataframes.append(df)
                        logger.info(f"Read CSV: {file_key} ({df.count()} records)")
                        
                    elif file_key.endswith(('.xlsx', '.xls')):
                        try:
                            dynamic_frame = glueContext.create_dynamic_frame.from_options(
                                connection_type="s3",
                                connection_options={"paths": [file_path]},
                                format="excel",
                                format_options={
                                    "withHeader": True,
                                    "inferSchema": True
                                }
                            )
                            df = dynamic_frame.toDF()
                            all_dataframes.append(df)
                            logger.info(f"Read Excel: {file_key} ({df.count()} records)")
                            
                        except Exception as excel_error:
                            logger.warning(f"Failed to read Excel file {file_key}: {str(excel_error)}")
                            try:
                                df = spark.read.option("header", "true") \
                                              .option("inferSchema", "true") \
                                              .csv(file_path)
                                all_dataframes.append(df)
                                logger.info(f"Read as CSV fallback: {file_key}")
                            except Exception as csv_error:
                                logger.error(f"Failed to read file {file_key}: {str(csv_error)}")
                                continue
                    
            except Exception as e:
                logger.warning(f"Error processing path {input_path}: {str(e)}")
                continue
        
        if all_dataframes:
            logger.info(f"Combining {len(all_dataframes)} dataframes")
            result_df = all_dataframes[0]
            for df in all_dataframes[1:]:
                if set(result_df.columns) == set(df.columns):
                    result_df = result_df.union(df.select(result_df.columns))
                else:
                    logger.warning("Schema mismatch detected, attempting to align columns")
                    common_columns = list(set(result_df.columns) & set(df.columns))
                    if common_columns:
                        result_df = result_df.select(common_columns).union(df.select(common_columns))
            return result_df
        return None
    
    def validate_orders_data(self, df: DataFrame) -> tuple:
        logger.info("Applying validation rules...")
        valid_df = df.filter(
            (col("order_id").isNotNull()) &
            (col("order_num").isNotNull()) &
            (col("user_id").isNotNull()) &
            (col("order_timestamp").isNotNull()) &
            (to_timestamp(col("order_timestamp")).isNotNull()) &
            (col("total_amount") > 0) &
            (col("total_amount") <= 50000.0) &
            (col("user_id") > 0) &
            (col("order_id") > 0)
        )
        invalid_df = df.subtract(valid_df)
        logger.info(f"Valid records: {valid_df.count()}")
        logger.info(f"Invalid records: {invalid_df.count()}")
        return valid_df, invalid_df
    
    def deduplicate_orders(self, df: DataFrame) -> DataFrame:
        logger.info("Deduplicating orders...")
        initial_count = df.count()
        window_spec = Window.partitionBy("order_id").orderBy(desc("order_timestamp"))
        deduped_df = df.withColumn("row_num", row_number().over(window_spec)) \
                      .filter(col("row_num") == 1) \
                      .drop("row_num")
        final_count = deduped_df.count()
        logger.info(f"Removed {initial_count - final_count} duplicate records")
        logger.info(f"Final record count: {final_count}")
        return deduped_df
    
    def transform_orders(self, df: DataFrame) -> DataFrame:
        logger.info("Applying transformations...")
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
        logger.info(f"Writing to Delta Lake: {table_path}")
        try:
            if DeltaTable.isDeltaTable(spark, table_path):
                logger.info("Performing MERGE operation (upsert)")
                delta_table = DeltaTable.forPath(spark, table_path)
                delta_table.alias("target").merge(
                    df.alias("source"),
                    "target.order_id = source.order_id"
                ).whenMatchedUpdateAll() \
                 .whenNotMatchedInsertAll() \
                 .execute()
                logger.info("MERGE operation completed")
            else:
                logger.info("Initial write with partitioning for performance")
                df.write.format("delta") \
                  .mode("overwrite") \
                  .partitionBy("year", "month") \
                  .save(table_path)
                logger.info("Initial Delta table created")
        except Exception as e:
            logger.error(f"Error writing to Delta Lake: {str(e)}")
            logger.info("Falling back to Parquet format...")
            df.write.mode("overwrite").partitionBy("year", "month").parquet(table_path)
    
    def log_rejected_records(self, invalid_df: DataFrame, reason: str):
        if invalid_df.count() > 0:
            logger.info(f"Logging {invalid_df.count()} rejected records")
            rejected_path = f"s3://{self.processed_bucket}/rejected/orders/{reason}/"
            invalid_df.withColumn("rejection_reason", lit(reason)) \
                     .withColumn("rejection_timestamp", current_timestamp()) \
                     .withColumn("job_run_id", lit(args['JOB_NAME'])) \
                     .write.mode("append").parquet(rejected_path)
            logger.info(f"Rejected records logged to: {rejected_path}")
    
    def archive_processed_files(self, source_paths):
        logger.info("Archiving processed files...")
        try:
            for source_path in source_paths:
                prefix = source_path.replace(f's3://{self.raw_bucket}/', '')
                response = self.s3_client.list_objects_v2(
                    Bucket=self.raw_bucket,
                    Prefix=prefix
                )
                if 'Contents' in response:
                    for obj in response['Contents']:
                        if obj['Key'].endswith(('.csv', '.xlsx', '.xls')):
                            copy_source = {'Bucket': self.raw_bucket, 'Key': obj['Key']}
                            archive_key = obj['Key'].replace('incoming/', 'archived/')
                            self.s3_client.copy_object(
                                CopySource=copy_source,
                                Bucket=self.raw_bucket,
                                Key=archive_key
                            )
                            self.s3_client.delete_object(
                                Bucket=self.raw_bucket,
                                Key=obj['Key']
                            )
                            logger.info(f"Archived: {obj['Key']} -> {archive_key}")
        except Exception as e:
            logger.warning(f"Archive operation failed: {str(e)}")
    
    def process_orders(self):
        logger.info("Starting Orders ETL Process")
        try:
            input_path = f"s3://{self.raw_bucket}/processing/orders/"
            logger.info(f"Reading converted CSV files from: {input_path}")
            raw_df = spark.read.option("header", "true") \
                            .option("inferSchema", "true") \
                            .csv(input_path)
            initial_count = raw_df.count()
            logger.info(f"Total records to process: {initial_count}")
            if initial_count == 0:
                logger.warning("No data found to process")
                return 0
            logger.info("Step 1: Applying comprehensive validation rules...")
            valid_df, invalid_df = self.validate_orders_data(raw_df)
            if invalid_df.count() > 0:
                logger.info(f"Step 2: Logging {invalid_df.count()} rejected records")
                self.log_rejected_records(invalid_df, "validation_failed")
            else:
                logger.info("Step 2: All records passed validation")
            logger.info("Step 3: Performing intelligent deduplication...")
            deduped_df = self.deduplicate_orders(valid_df)
            if deduped_df.count() == 0:
                logger.error("No records remaining after deduplication")
                return 0
            logger.info("Step 4: Applying business transformations...")
            transformed_df = self.transform_orders(deduped_df)
            logger.info("Step 5: Writing to Delta Lake with ACID guarantees...")
            delta_path = f"s3://{self.processed_bucket}/lakehouse-dwh/orders/"
            self.write_to_delta_lake(transformed_df, delta_path)
            logger.info("Step 6: Archiving successfully processed files...")
            self.archive_processed_files([f"s3://{self.raw_bucket}/incoming/orders/"])
            final_count = transformed_df.count()
            success_rate = (final_count / initial_count) * 100 if initial_count > 0 else 0
            logger.info("=== Processing Summary ===")
            logger.info(f"Initial records: {initial_count}")
            logger.info(f"Invalid records: {invalid_df.count()}")
            logger.info(f"Duplicates removed: {valid_df.count() - deduped_df.count()}")
            logger.info(f"Final processed records: {final_count}")
            logger.info(f"Success rate: {success_rate:.2f}%")
            logger.info("Orders ETL completed successfully!")
            logger.info(f"Delta Lake table updated at: {delta_path}")
            return final_count
        except Exception as e:
            logger.error(f"Critical error in Orders ETL: {str(e)}")
            logger.error(f"Error details: {type(e).__name__}")
            logger.error("Full traceback:\n" + traceback.format_exc())
            raise e

if __name__ == "__main__":
    etl = OrdersETL(
        raw_bucket=args['RAW_BUCKET'],
        processed_bucket=args['PROCESSED_BUCKET'],
        environment=args['ENVIRONMENT'],
        glue_database=args['GLUE_DATABASE']
    )
    processed_count = etl.process_orders()
    logger.info(f"Orders ETL job completed. Processed {processed_count} records.")

job.commit()
