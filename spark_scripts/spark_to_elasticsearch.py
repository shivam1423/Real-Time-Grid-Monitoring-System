"""
PySpark to Elasticsearch Writer
Writes processed energy grid data to Elasticsearch for visualization.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, expr, from_unixtime, unix_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    IntegerType, BooleanType, TimestampType
)
import os
import yaml


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


def create_spark_session():
    """Initialize Spark session with Elasticsearch support"""
    return SparkSession.builder \
        .appName("EnergyGridToElasticsearch") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
                "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
        .config("spark.es.nodes", "localhost") \
        .config("spark.es.port", "9200") \
        .config("spark.es.nodes.wan.only", "true") \
        .config("spark.es.batch.size.entries", "1000") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()


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
        StructField("timestamp", TimestampType(), True),
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


def write_to_elasticsearch(df, index_name, checkpoint_path):
    """
    Write streaming dataframe to Elasticsearch
    Uses foreach batch to write micro-batches
    """
    
    def write_batch_to_es(batch_df, batch_id):
        """Write each micro-batch to Elasticsearch"""
        if not batch_df.isEmpty():
            # Use selectExpr for more efficient flattening
            flattened_df = batch_df.selectExpr(
                "timestamp",
                "region",
                "grid_id",
                "demand_mw",
                "total_generation_mw",
                "generation_mix_percent.Solar as solar_percent",
                "generation_mix_percent.Wind as wind_percent",
                "generation_mix_percent.Natural_Gas as natural_gas_percent",
                "generation_mix_percent.Coal as coal_percent",
                "generation_mix_percent.Nuclear as nuclear_percent",
                "generation_mix_percent.Hydro as hydro_percent",
                "generation_mw.Solar as solar_mw",
                "generation_mw.Wind as wind_mw",
                "generation_mw.Natural_Gas as natural_gas_mw",
                "generation_mw.Coal as coal_mw",
                "generation_mw.Nuclear as nuclear_mw",
                "generation_mw.Hydro as hydro_mw",
                "grid_frequency_hz",
                "voltage_kv",
                "carbon_intensity_gco2_kwh",
                "reserve_margin_percent",
                "temperature_celsius",
                "hour_of_day",
                "renewable_percentage",
                "generation_cost_per_hour",
                "is_anomaly",
                "alert_level",
                "concat(region, '_', grid_id, '_', cast(timestamp as string)) as es_id"
            )
            
            # Write to Elasticsearch with optimized settings
            flattened_df.write \
                .format("org.elasticsearch.spark.sql") \
                .option("es.resource", index_name) \
                .option("es.mapping.id", "es_id") \
                .option("es.batch.size.entries", "1000") \
                .option("es.batch.size.bytes", "1mb") \
                .option("es.batch.write.retry.count", "3") \
                .option("es.batch.write.retry.wait", "10s") \
                .mode("append") \
                .save()
            
            print(f"Written batch {batch_id} to Elasticsearch")
    
    return df.writeStream \
        .foreachBatch(write_batch_to_es) \
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
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    index_name = config['elasticsearch']['index_name']
    topic = config['streaming']['processed_topic']
    
    # Ensure checkpoint directory exists
    checkpoint_path = f"/tmp/checkpoint/elasticsearch_{index_name}"
    ensure_checkpoint_directory(checkpoint_path)
    
    print(f"Starting Elasticsearch writer...")
    print(f"Reading from Kafka topic: {topic}")
    print(f"Writing to Elasticsearch index: {index_name}")
    
    # Read from Kafka with backpressure handling
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9093") \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", 500) \
        .option("kafka.consumer.session.timeout.ms", "30000") \
        .option("kafka.consumer.heartbeat.interval.ms", "10000") \
        .load()
    
    # Parse JSON data
    schema = get_schema()
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Convert timestamp to epoch milliseconds for Elasticsearch
    # When casting timestamp to long in Spark, it gives seconds since epoch
    # Multiply by 1000 to get milliseconds for Elasticsearch
    parsed_df = parsed_df.withColumn(
        "timestamp",
        (col("timestamp").cast("long") * 1000).cast("long")
    )
    
    # Write to Elasticsearch
    query = write_to_elasticsearch(parsed_df, index_name, checkpoint_path)
    
    # Wait for termination
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping Elasticsearch writer...")
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()

