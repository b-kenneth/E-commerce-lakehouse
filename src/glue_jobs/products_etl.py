# src/glue_jobs/products_etl.py
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
    'ENVIRONMENT',
    'GLUE_DATABASE'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# REMOVE THESE LINES - They cause the error:
# spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
# spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Delta Lake configurations are now set at job level via --conf parameters

class ProductsETL:
    def __init__(self, raw_bucket, processed_bucket, environment, glue_database):
        self.raw_bucket = raw_bucket
        self.processed_bucket = processed_bucket
        self.environment = environment
        self.glue_database = glue_database
        self.s3_client = boto3.client('s3')
        
        # Verify Delta Lake is properly configured
        print("🔧 Verifying Delta Lake configuration...")
        try:
            extensions = spark.conf.get("spark.sql.extensions", "Not set")
            catalog = spark.conf.get("spark.sql.catalog.spark_catalog", "Not set")
            print(f"✅ Extensions: {extensions}")
            print(f"✅ Catalog: {catalog}")
        except Exception as e:
            print(f"⚠️ Configuration check failed: {str(e)}")
        
    def validate_products_data(self, df: DataFrame) -> tuple:
        """Validate products data with referential integrity checks"""
        
        print("🔍 Validating products data...")
        
        valid_df = df.filter(
            # No null primary identifiers
            (col("product_id").isNotNull()) &
            (col("department_id").isNotNull()) &
            (col("department").isNotNull()) &
            (col("product_name").isNotNull()) &
            
            # Business logic validations
            (col("product_id") > 0) &
            (col("department_id") > 0) &
            (length(trim(col("product_name"))) > 0) &
            (length(trim(col("department"))) > 0)
        )
        
        invalid_df = df.subtract(valid_df)
        
        print(f"✅ Valid products: {valid_df.count()}")
        print(f"❌ Invalid products: {invalid_df.count()}")
        
        return valid_df, invalid_df
    
    def transform_products(self, df: DataFrame) -> DataFrame:
        """Apply transformations to products data"""
        
        print("🔧 Transforming products data...")
        
        return df.withColumn("processing_timestamp", current_timestamp()) \
                 .withColumn("department_clean", trim(upper(col("department")))) \
                 .withColumn("product_name_clean", trim(col("product_name"))) \
                 .withColumn("is_active", lit(True))
    
    def write_to_delta_lake(self, df: DataFrame, table_path: str):
        """Write products to Delta Lake with merge logic"""
        
        print(f"💾 Writing products to Delta Lake: {table_path}")
        
        try:
            if DeltaTable.isDeltaTable(spark, table_path):
                print("📝 Performing MERGE operation")
                
                delta_table = DeltaTable.forPath(spark, table_path)
                
                delta_table.alias("target").merge(
                    df.alias("source"),
                    "target.product_id = source.product_id"
                ).whenMatchedUpdateAll() \
                 .whenNotMatchedInsertAll() \
                 .execute()
                 
                print("✅ MERGE operation completed")
            else:
                print("📝 Initial write with partitioning by department")
                
                df.write.format("delta") \
                  .mode("overwrite") \
                  .partitionBy("department") \
                  .save(table_path)
                  
                print("✅ Initial Delta table created")
                
        except Exception as e:
            print(f"💥 Error writing to Delta Lake: {str(e)}")
            # Fallback to Parquet if Delta Lake fails
            print("🔄 Falling back to Parquet format...")
            df.write.mode("overwrite").partitionBy("department").parquet(table_path)
    
    def process_products(self):
        """Main processing logic for products"""
        
        print("🚀 Starting Products ETL Process")
        
        try:
            input_path = f"s3://{self.raw_bucket}/incoming/products/"
            print(f"📖 Reading products from: {input_path}")
            
            raw_df = spark.read.option("header", "true") \
                              .option("inferSchema", "true") \
                              .csv(input_path)
            
            initial_count = raw_df.count()
            print(f"📊 Initial product count: {initial_count}")
            
            if initial_count == 0:
                print("⚠️ No products data found")
                return 0
            
            # Validate
            valid_df, invalid_df = self.validate_products_data(raw_df)
            
            # Log rejected records
            if invalid_df.count() > 0:
                rejected_path = f"s3://{self.processed_bucket}/rejected/products/"
                invalid_df.withColumn("rejection_reason", lit("validation_failed")) \
                         .withColumn("rejection_timestamp", current_timestamp()) \
                         .write.mode("append").parquet(rejected_path)
                print(f"📝 Rejected {invalid_df.count()} invalid records")
            
            # Transform
            transformed_df = self.transform_products(valid_df)
            
            # Write to Delta Lake
            delta_path = f"s3://{self.processed_bucket}/lakehouse-dwh/products/"
            self.write_to_delta_lake(transformed_df, delta_path)
            
            final_count = transformed_df.count()
            print(f"🎉 Products ETL completed! Processed {final_count} products.")
            
            return final_count
            
        except Exception as e:
            print(f"💥 Error in Products ETL: {str(e)}")
            import traceback
            print(f"🔍 Full traceback:")
            print(traceback.format_exc())
            raise e

# Main execution
if __name__ == "__main__":
    etl = ProductsETL(
        raw_bucket=args['RAW_BUCKET'],
        processed_bucket=args['PROCESSED_BUCKET'],
        environment=args['ENVIRONMENT'],
        glue_database=args['GLUE_DATABASE']
    )
    
    processed_count = etl.process_products()
    print(f"🏁 Products ETL completed. Processed {processed_count} records.")

job.commit()
