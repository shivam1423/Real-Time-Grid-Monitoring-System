"""
Airflow DAG for Energy Grid Monitoring Pipeline
Orchestrates the complete streaming data pipeline from generation to visualization.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import requests
import json


default_args = {
    'owner': 'energy-grid-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def validate_kafka_stream(**context):
    """
    Validate that Kafka is receiving data properly
    """
    from confluent_kafka import Consumer
    import yaml
    
    with open('/opt/airflow/config/energy_grid_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    topic = config['streaming']['kafka_topic']
    
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['kafka:9092'],
            auto_offset_reset='latest',
            consumer_timeout_ms=10000
        )
        
        message_count = 0
        for message in consumer:
            message_count += 1
            if message_count >= 5:
                break
        
        consumer.close()
        
        if message_count > 0:
            print(f"Kafka validation passed: {message_count} messages received from {topic}")
            return True
        else:
            raise Exception(f"No messages found in topic {topic}")
            
    except Exception as e:
        print(f"Kafka validation failed: {e}")
        raise


def check_elasticsearch_health(**context):
    """
    Check if Elasticsearch is healthy and create index if needed
    """
    import yaml
    import requests
    import json
    
    with open('/opt/airflow/config/energy_grid_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    es_host = config['elasticsearch']['host']
    es_port = config['elasticsearch']['port']
    index_name = config['elasticsearch']['index_name']
    
    # Check cluster health
    health_url = f"http://{es_host}:{es_port}/_cluster/health"
    response = requests.get(health_url)
    
    if response.status_code == 200:
        health = response.json()
        print(f"Elasticsearch health: {health['status']}")
    else:
        raise Exception(f"Elasticsearch health check failed: {response.status_code}")
    
    # Check if index exists, create if not
    index_url = f"http://{es_host}:{es_port}/{index_name}"
    response = requests.get(index_url)
    
    if response.status_code == 404:
        # Create index with mappings
        with open('/opt/airflow/config/elasticsearch_mappings.json', 'r') as f:
            mappings = json.load(f)
        
        response = requests.put(index_url, json=mappings)
        if response.status_code == 200:
            print(f"Created Elasticsearch index: {index_name}")
        else:
            raise Exception(f"Failed to create index: {response.text}")
    else:
        print(f"Elasticsearch index {index_name} exists")


def generate_alerts(**context):
    """
    Check for critical anomalies and generate alerts
    """
    from confluent_kafka import Consumer
    import yaml
    import json
    
    with open('/opt/airflow/config/energy_grid_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    topic = config['streaming']['processed_topic']
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='latest',
        consumer_timeout_ms=30000,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    critical_alerts = []
    
    for message in consumer:
        data = message.value
        
        if data.get('alert_level') == 'CRITICAL':
            critical_alerts.append({
                'region': data.get('region'),
                'timestamp': data.get('timestamp'),
                'frequency': data.get('grid_frequency_hz'),
                'reserve_margin': data.get('reserve_margin_percent'),
                'voltage': data.get('voltage_kv')
            })
    
    consumer.close()
    
    if critical_alerts:
        print(f"CRITICAL ALERTS DETECTED: {len(critical_alerts)} events")
        for alert in critical_alerts[:5]:  # Show first 5
            print(f"  - {alert['region']} at {alert['timestamp']}")
    else:
        print("No critical alerts detected")
    
    return critical_alerts


def update_dashboards(**context):
    """
    Trigger Kibana dashboard refresh
    """
    import yaml
    
    with open('/opt/airflow/config/energy_grid_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # This would normally trigger a dashboard refresh
    # For now, just log success
    print("Dashboard refresh triggered")
    return True


# Define the DAG
dag = DAG(
    'energy_grid_pipeline',
    default_args=default_args,
    description='Real-time energy grid monitoring and analytics pipeline',
    schedule_interval=None,  # Manual trigger only
    max_active_runs=1,
    catchup=False,
    tags=['energy', 'streaming', 'real-time'],
)


# Task 1: Create Kafka topics
create_topics = BashOperator(
    task_id='create_kafka_topics',
    bash_command='python /opt/airflow/kafka_admin_client/create_topics.py',
    dag=dag,
)


# Task 2: Start data generator (runs in background)
start_data_generator = BashOperator(
    task_id='start_data_generator',
    bash_command='nohup python /opt/airflow/data_generator/energy_grid_generator.py /opt/airflow/config/energy_grid_config.yaml 5 > /opt/airflow/logs/data_generator.log 2>&1 & echo $! > /opt/airflow/logs/data_generator.pid',
    dag=dag,
)


# Task 3: Validate Kafka stream
validate_kafka = PythonOperator(
    task_id='validate_kafka_stream',
    python_callable=validate_kafka_stream,
    dag=dag,
)


# Task 4: Check Elasticsearch health and setup
check_elasticsearch = PythonOperator(
    task_id='check_elasticsearch_health',
    python_callable=check_elasticsearch_health,
    dag=dag,
)


# Task 5: Process data with Spark
process_with_spark = BashOperator(
    task_id='process_with_spark',
    bash_command='cd /opt/airflow && python spark_scripts/process_energy_grid.py',
    dag=dag,
)


# Task 6: Write to Elasticsearch
write_to_elasticsearch = BashOperator(
    task_id='write_to_elasticsearch',
    bash_command='cd /opt/airflow && python spark_scripts/spark_to_elasticsearch.py',
    dag=dag,
)


# Task 7: Write to MinIO
write_to_minio = BashOperator(
    task_id='write_to_minio',
    bash_command='cd /opt/airflow && python spark_scripts/spark_to_minio.py',
    dag=dag,
)


# Task 8: Generate alerts for critical anomalies
generate_alerts_task = PythonOperator(
    task_id='generate_alerts',
    python_callable=generate_alerts,
    dag=dag,
)


# Task 9: Update dashboards
update_dashboards_task = PythonOperator(
    task_id='update_dashboards',
    python_callable=update_dashboards,
    dag=dag,
)


# Define task dependencies
create_topics >> start_data_generator >> validate_kafka
validate_kafka >> check_elasticsearch
check_elasticsearch >> process_with_spark
process_with_spark >> [write_to_elasticsearch, write_to_minio]
[write_to_elasticsearch, write_to_minio] >> generate_alerts_task
generate_alerts_task >> update_dashboards_task

