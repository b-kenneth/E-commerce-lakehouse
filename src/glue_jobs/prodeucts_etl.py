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

class ProductsETL:
    def __init__(self, raw_bucket, processed_bucket, environment):
        self.raw_bucket = raw_bucket
        self.processed_bucket = processed_bucket
        self.environment = environment
        
    def validate_products_data(self, df: DataFrame) -> tuple:
        """Validate products data"""
        
        valid_df = df.filter(
            (col("product_id").isNotNull()) &
            (col("department_id").isNotNull()) &
            (col("department").isNotNull()) &
            (col("product_name").isNotNull()) &
            # Ensure positive IDs
            (col("product_id") > 0) &
            (col("department_id") > 0) &
            # Valid department names
            (col("department").isin(["Electronics", "Clothing", "Home", "Books", "Sports", "Toys"]))
        )
        
        invalid_df = df.subtract(valid_df)
        return valid_df, invalid_df
    
    def transform_products(self, df: DataFrame) -> DataFrame:
        """Apply transformations to products data"""
        return df.withColumn("processing_timestamp", current_timestamp()) \
                 .withColumn("department_name_clean", trim(upper(col("department"))))
    
    def write_to_delta(self, df: DataFrame, table_path: str):
        """Write DataFrame to Delta Lake with merge logic"""
        
        if DeltaTable.isDeltaTable(spark, table_path):
            delta_table = DeltaTable.forPath(spark, table_path)
            
            delta_table.alias("target").merge(
                df.alias("source"),
                "target.product_id = source.product_id"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
        else:
            df.write.format("delta") \
              .mode("overwrite") \
              .partitionBy("department") \
              .save(table_path)
    
    def process_products(self, file_path: str):
        """Main processing logic for products"""
        
        raw_df = spark.read.option("header", "true") \
                          .option("inferSchema", "true") \
                          .csv(file_path)
        
        print(f"Processing {raw_df.count()} raw products records")
        
        # Validate data
        valid_df, invalid_df = self.validate_products_data(raw_df)
        
        if invalid_df.count() > 0:
            print(f"Found {invalid_df.count()} invalid records")
            # Log rejected records
            rejected_path = f"s3://{self.processed_bucket}/rejected/products/"
            invalid_df.withColumn("rejection_reason", lit("validation_failed")) \
                     .withColumn("rejection_timestamp", current_timestamp()) \
                     .write.mode("append").parquet(rejected_path)
        
        # No deduplication needed for products as product_id should be unique
        # Transform
        transformed_df = self.transform_products(valid_df)
        
        # Write to Delta Lake
        delta_path = f"s3://{self.processed_bucket}/lakehouse-dwh/products/"
        self.write_to_delta(transformed_df, delta_path)
        
        print(f"Successfully processed products to {delta_path}")
        return transformed_df.count()

# Main execution
if __name__ == "__main__":
    etl = ProductsETL(
        raw_bucket=args['RAW_BUCKET'],
        processed_bucket=args['PROCESSED_BUCKET'],
        environment=args['ENVIRONMENT']
    )
    
    input_path = f"s3://{args['RAW_BUCKET']}/products/*.csv"
    processed_count = etl.process_products(input_path)
    
    print(f"Products ETL job completed. Processed {processed_count} records.")

job.commit()
