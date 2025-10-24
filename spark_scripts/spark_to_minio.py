"""
PySpark to MinIO Writer
Stores energy grid data in MinIO object storage with partitioning.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, year, month, dayofmonth, hour as hour_func
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    IntegerType, BooleanType, TimestampType
)
import os
import yaml
import sys
from minio import Minio
from minio.error import S3Error

# Add config directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
from environment_config import get_minio_endpoint, get_minio_credentials, get_kafka_bootstrap_servers


def ensure_checkpoint_directory(checkpoint_path: str):
    """Create checkpoint directory if it doesn't exist"""
    try:
        os.makedirs(checkpoint_path, exist_ok=True)
        print(f"Checkpoint directory ready: {checkpoint_path}")
    except PermissionError:
        print(f"Permission denied creating checkpoint directory: {checkpoint_path}")
        print("Please ensure the directory is writable or run with appropriate permissions")
        raise
    except Exception as e:
        print(f"Error creating checkpoint directory: {e}")
        raise


def create_spark_session(minio_endpoint: str, access_key: str, secret_key: str):
    """Initialize Spark session with S3/MinIO support using provided endpoint and credentials."""
    s3a_endpoint = (
        minio_endpoint if minio_endpoint.startswith("http") else f"http://{minio_endpoint}"
    )
    return SparkSession.builder \
        .appName("EnergyGridToMinIO") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", s3a_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()


def ensure_bucket_exists(bucket_name, endpoint: str, access_key: str, secret_key: str):
    """Create MinIO bucket if it doesn't exist"""
    try:
        # MinIO Python client expects host:port without scheme
        endpoint_no_scheme = endpoint.replace("http://", "").replace("https://", "")
        client = Minio(
            endpoint_no_scheme,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        
        if not client.bucket_exists(bucket_name):
            try:
                client.make_bucket(bucket_name)
                print(f"Created bucket: {bucket_name}")
            except S3Error as e:
                if "BucketAlreadyOwnedByYou" in str(e) or "BucketAlreadyExists" in str(e):
                    print(f"Bucket {bucket_name} already exists")
                else:
                    print(f"Error creating bucket: {e}")
                    raise
        else:
            print(f"Bucket {bucket_name} already exists")
    except S3Error as e:
        print(f"Error with MinIO: {e}")
        raise


def get_schema():
    """Define schema for energy grid data"""
    generation_schema = StructType([
        StructField("Solar", FloatType(), True),
        StructField("Wind", FloatType(), True),
        StructField("Natural_Gas", FloatType(), True),
        StructField("Coal", FloatType(), True),
        StructField("Nuclear", FloatType(), True),
        StructField("Hydro", FloatType(), True)
    ])
    
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("region", StringType(), True),
        StructField("grid_id", StringType(), True),
        StructField("demand_mw", FloatType(), True),
        StructField("total_generation_mw", FloatType(), True),
        StructField("generation_mix_percent", generation_schema, True),
        StructField("generation_mw", generation_schema, True),
        StructField("grid_frequency_hz", FloatType(), True),
        StructField("voltage_kv", FloatType(), True),
        StructField("carbon_intensity_gco2_kwh", FloatType(), True),
        StructField("reserve_margin_percent", FloatType(), True),
        StructField("temperature_celsius", FloatType(), True),
        StructField("hour_of_day", IntegerType(), True),
        StructField("renewable_percentage", FloatType(), True),
        StructField("generation_cost_per_hour", FloatType(), True),
        StructField("is_anomaly", BooleanType(), True),
        StructField("alert_level", StringType(), True)
    ])


def write_to_minio(df, bucket_name, prefix, checkpoint_path):
    """
    Write streaming dataframe to MinIO with partitioning
    Partitions by year/month/day/hour/region
    """
    
    def write_batch_to_minio(batch_df, batch_id):
        """Write each micro-batch to MinIO"""
        if batch_df.count() > 0:
            # Add partition columns
            partitioned_df = batch_df \
                .withColumn("year", year(col("timestamp"))) \
                .withColumn("month", month(col("timestamp"))) \
                .withColumn("day", dayofmonth(col("timestamp"))) \
                .withColumn("hour", hour_func(col("timestamp")))
            
            # Write to S3/MinIO with partitioning
            s3_path = f"s3a://{bucket_name}/{prefix}"
            
            partitioned_df.write \
                .partitionBy("year", "month", "day", "hour", "region") \
                .mode("append") \
                .parquet(s3_path)
            
            print(f"Written batch {batch_id} with {batch_df.count()} records to MinIO")
    
    return df.writeStream \
        .foreachBatch(write_batch_to_minio) \
        .option("checkpointLocation", checkpoint_path) \
        .start()


def main():
    """Main execution function"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    config_path = os.path.join(project_root, 'config', 'energy_grid_config.yaml')
    
    # Load configuration
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    bucket_name = config['minio']['bucket_name']
    processed_prefix = config['minio']['processed_data_prefix']
    # Use centralized configuration
    minio_endpoint = get_minio_endpoint()
    minio_access_key, minio_secret_key = get_minio_credentials()
    topic = config['streaming']['processed_topic']
    print(f"MinIO endpoint: {minio_endpoint}")
    # Ensure bucket exists
    ensure_bucket_exists(bucket_name, minio_endpoint, minio_access_key, minio_secret_key)
    
    # Create Spark session
    spark = create_spark_session(minio_endpoint, minio_access_key, minio_secret_key)
    spark.sparkContext.setLogLevel("WARN")
    
    # Ensure checkpoint directory exists
    checkpoint_path = f"/tmp/checkpoint/minio_{processed_prefix}"
    ensure_checkpoint_directory(checkpoint_path)
    
    print(f"Starting MinIO writer...")
    print(f"Reading from Kafka topic: {topic}")
    print(f"Writing to MinIO bucket: {bucket_name}/{processed_prefix}")
    
    # Read from Kafka using centralized configuration
    kafka_bootstrap = get_kafka_bootstrap_servers()
    print(f"Kafka bootstrap: {kafka_bootstrap}")
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .option("kafka.consumer.session.timeout.ms", "30000") \
        .option("kafka.consumer.heartbeat.interval.ms", "10000") \
        .load()
    
    # Parse JSON data
    schema = get_schema()
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Convert timestamp to proper format
    parsed_df = parsed_df.withColumn(
        "timestamp",
        col("timestamp").cast(TimestampType())
    )
    
    # Write to MinIO
    query = write_to_minio(parsed_df, bucket_name, processed_prefix, checkpoint_path)
    
    # Wait for termination
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping MinIO writer...")
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()

