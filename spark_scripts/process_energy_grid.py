"""
PySpark Energy Grid Processing
Real-time streaming data processing with anomaly detection and aggregations.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, max as spark_max, min as spark_min,
    sum as spark_sum, count, expr, udf, struct, current_timestamp, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, 
    IntegerType, BooleanType, TimestampType, MapType
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
    """Initialize Spark session with Kafka support"""
    return SparkSession.builder \
        .appName("EnergyGridProcessing") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
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


def detect_anomaly_udf(frequency, reserve_margin, voltage):
    """
    UDF for anomaly detection
    Flags anomalies based on grid stability metrics
    """
    is_anomaly = False
    alert_level = "NORMAL"
    
    freq_deviation = abs(frequency - 60.0)
    
    # Critical frequency deviation
    if freq_deviation > 0.025:
        is_anomaly = True
        alert_level = "CRITICAL"
    # Warning frequency deviation
    elif freq_deviation > 0.015:
        is_anomaly = True
        alert_level = "WARNING"
    
    # Low reserve margin
    if reserve_margin < 5:
        is_anomaly = True
        alert_level = "CRITICAL"
    elif reserve_margin < 10:
        is_anomaly = True
        if alert_level == "NORMAL":
            alert_level = "WARNING"
    
    # Voltage out of range
    if voltage < 335 or voltage > 355:
        is_anomaly = True
        if alert_level == "NORMAL":
            alert_level = "WARNING"
    
    return (is_anomaly, alert_level)


def calculate_carbon_intensity_udf(generation_mix):
    """
    Calculate weighted carbon intensity
    Returns grams CO2 per kWh
    """
    carbon_coefficients = {
        'Coal': 920,
        'Natural_Gas': 450,
        'Solar': 50,
        'Wind': 10,
        'Nuclear': 10,
        'Hydro': 20
    }
    
    total_carbon = sum(
        generation_mix.get(source, 0) * carbon_coefficients[source] / 100
        for source in carbon_coefficients.keys()
    )
    
    return round(total_carbon, 2)


def calculate_generation_cost_udf(generation_mw):
    """
    Calculate total generation cost per hour
    Returns cost in dollars
    """
    generation_costs = {
        'Coal': 60,
        'Natural_Gas': 70,
        'Solar': 30,
        'Wind': 25,
        'Nuclear': 30,
        'Hydro': 40
    }
    
    total_cost = sum(
        generation_mw.get(source, 0) * generation_costs[source]
        for source in generation_costs.keys()
    )
    
    return round(total_cost, 2)


def calculate_renewable_percentage_udf(generation_mix):
    """
    Calculate percentage of generation from renewable sources
    """
    renewable_sources = ['Solar', 'Wind', 'Hydro']
    renewable_total = sum(
        generation_mix.get(source, 0) for source in renewable_sources
    )
    total = sum(generation_mix.values())
    
    if total > 0:
        return round((renewable_total / total) * 100, 2)
    return 0.0


def process_streaming_data(spark, config):
    """
    Main processing pipeline for energy grid streaming data
    """
    
    # Read from Kafka with backpressure handling
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9093") \
        .option("subscribe", config['streaming']['kafka_topic']) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .option("kafka.consumer.session.timeout.ms", "30000") \
        .option("kafka.consumer.heartbeat.interval.ms", "10000") \
        .load()
    
    # Parse JSON data
    schema = get_schema()
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Convert timestamp string to timestamp type
    # Handle ISO 8601 format with timezone offset (e.g., 2024-12-15T12:36:30+00:00)
    parsed_df = parsed_df.withColumn(
        "timestamp",
        to_timestamp(col("timestamp"))
    )
    
    # Add processing timestamp
    parsed_df = parsed_df.withColumn("processed_at", current_timestamp())
    
    # Calculate additional metrics
    # Note: UDFs are already calculated in the generator, but we can recalculate for validation
    
    # Create windowed aggregations - 5 minute windows
    windowed_df = parsed_df \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            col("region")
        ) \
        .agg(
            avg("demand_mw").alias("avg_demand_mw"),
            spark_max("demand_mw").alias("peak_demand_mw"),
            spark_min("demand_mw").alias("min_demand_mw"),
            avg("grid_frequency_hz").alias("avg_frequency_hz"),
            avg("carbon_intensity_gco2_kwh").alias("avg_carbon_intensity"),
            avg("renewable_percentage").alias("avg_renewable_percentage"),
            spark_sum(col("is_anomaly").cast("int")).alias("anomaly_count"),
            count("*").alias("record_count")
        )
    
    # Hourly aggregations for trend analysis
    hourly_df = parsed_df \
        .withWatermark("timestamp", "1 hour") \
        .groupBy(
            window(col("timestamp"), "1 hour"),
            col("region")
        ) \
        .agg(
            avg("demand_mw").alias("hourly_avg_demand"),
            spark_max("demand_mw").alias("hourly_peak_demand"),
            avg("renewable_percentage").alias("hourly_renewable_pct"),
            avg("carbon_intensity_gco2_kwh").alias("hourly_carbon_intensity"),
            spark_sum("generation_cost_per_hour").alias("total_generation_cost")
        )
    
    return parsed_df, windowed_df, hourly_df


def write_to_kafka(df, topic, checkpoint_location):
    """Write processed data back to Kafka"""
    return df \
        .selectExpr("CAST(region AS STRING) AS key", "to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9093") \
        .option("topic", topic) \
        .option("checkpointLocation", checkpoint_location) \
        .outputMode("append") \
        .start()


def write_to_console(df, output_mode="append"):
    """Write data to console for debugging"""
    return df \
        .writeStream \
        .format("console") \
        .outputMode(output_mode) \
        .option("truncate", False) \
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
    
    print("Starting Energy Grid Streaming Processing...")
    print(f"Reading from topic: {config['streaming']['kafka_topic']}")
    print(f"Writing to topic: {config['streaming']['processed_topic']}")
    
    # Ensure checkpoint directories exist
    checkpoint_base = "/tmp/checkpoint"
    ensure_checkpoint_directory(checkpoint_base)
    ensure_checkpoint_directory(f"{checkpoint_base}/raw")
    ensure_checkpoint_directory(f"{checkpoint_base}/windowed")
    ensure_checkpoint_directory(f"{checkpoint_base}/hourly")
    
    # Process streaming data
    raw_df, windowed_df, hourly_df = process_streaming_data(spark, config)
    
    # Write raw processed data to Kafka
    raw_query = write_to_kafka(
        raw_df,
        config['streaming']['processed_topic'],
        f"{checkpoint_base}/raw"
    )
    
    # Write windowed aggregations to console (can be changed to Kafka/ES)
    windowed_query = write_to_console(windowed_df, output_mode="complete")
    
    # Write hourly aggregations to console
    hourly_query = write_to_console(hourly_df, output_mode="complete")
    
    # Wait for termination
    try:
        raw_query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping streaming queries...")
        raw_query.stop()
        windowed_query.stop()
        hourly_query.stop()
        spark.stop()


if __name__ == "__main__":
    main()

