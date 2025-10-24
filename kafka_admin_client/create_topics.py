"""
Kafka Topic Administration
Creates and configures Kafka topics for the energy grid streaming pipeline.
"""

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException
import yaml
import os
import sys

# Add config directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
from environment_config import get_kafka_bootstrap_servers

def create_energy_grid_topics(config_path='../config/energy_grid_config.yaml'):
    """Create Kafka topics with appropriate configurations"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    config_path = os.path.join(project_root, 'config', 'energy_grid_config.yaml')
    
    # Load configuration
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Use centralized configuration
    bootstrap = get_kafka_bootstrap_servers()
    admin_client = AdminClient({
        'bootstrap.servers': bootstrap,
        'client.id': 'energy_grid_admin'
    })
    
    topics = [
        NewTopic(
            topic=config['streaming']['kafka_topic'],
            num_partitions=config['streaming']['kafka_partitions'],
            replication_factor=1,
            config={
                'retention.ms': str(7 * 24 * 60 * 60 * 1000),  # 7 days
                'compression.type': 'gzip',
                'segment.bytes': str(1073741824),  # 1 GB
                'cleanup.policy': 'delete'
            }
        ),
        NewTopic(
            topic=config['streaming']['processed_topic'],
            num_partitions=config['streaming']['kafka_partitions'],
            replication_factor=1,
            config={
                'retention.ms': str(30 * 24 * 60 * 60 * 1000),  # 30 days
                'compression.type': 'gzip',
                'segment.bytes': str(1073741824),
                'cleanup.policy': 'delete'
            }
        )
    ]
    
    try:
        # Create topics
        fs = admin_client.create_topics(topics)
        
        # Wait for each topic creation to complete
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print(f"Created topic: {topic}")
            except KafkaException as e:
                if e.args[0].code() == 36:  # TopicAlreadyExistsError
                    print(f"Topic {topic} already exists, skipping creation")
                else:
                    print(f"Error creating topic {topic}: {e}")
    except Exception as e:
        print(f"Error creating topics: {e}")


def list_topics():
    """List all Kafka topics"""
    bootstrap = get_kafka_bootstrap_servers()
    admin_client = AdminClient({
        'bootstrap.servers': bootstrap,
        'client.id': 'energy_grid_admin'
    })
    
    try:
        metadata = admin_client.list_topics(timeout=10)
        topics = list(metadata.topics.keys())
        print(f"Available topics: {topics}")
    except Exception as e:
        print(f"Error listing topics: {e}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'list':
        list_topics()
    else:
        create_energy_grid_topics()

