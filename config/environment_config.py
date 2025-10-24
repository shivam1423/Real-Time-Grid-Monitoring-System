"""
Environment Configuration Utility
Centralized configuration for different deployment environments.
"""

import os
from typing import Dict, Any


def get_kafka_bootstrap_servers() -> str:
    """
    Get Kafka bootstrap servers based on environment.
    Returns appropriate bootstrap server for container vs host deployment.
    """
    # Check if running in container
    in_container = os.path.exists('/.dockerenv')
    
    # Environment variable override
    if 'KAFKA_BOOTSTRAP' in os.environ:
        return os.environ['KAFKA_BOOTSTRAP']
    
    # Default based on environment
    if in_container:
        return 'kafka:9092'
    else:
        return 'localhost:9093'


def get_minio_endpoint() -> str:
    """
    Get MinIO endpoint based on environment.
    """
    # Environment variable override
    if 'MINIO_ENDPOINT' in os.environ:
        return os.environ['MINIO_ENDPOINT']
    
    # Check if running in container
    in_container = os.path.exists('/.dockerenv')
    
    if in_container:
        return 'minio:9000'
    else:
        return 'localhost:9002'


def get_minio_credentials() -> tuple[str, str]:
    """
    Get MinIO credentials from environment or defaults.
    """
    access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
    return access_key, secret_key


def get_elasticsearch_host() -> str:
    """
    Get Elasticsearch host based on environment.
    """
    # Environment variable override
    if 'ELASTICSEARCH_HOST' in os.environ:
        return os.environ['ELASTICSEARCH_HOST']
    
    # Check if running in container
    in_container = os.path.exists('/.dockerenv')
    
    if in_container:
        return 'elasticsearch'
    else:
        return 'localhost'


def get_elasticsearch_port() -> int:
    """
    Get Elasticsearch port.
    """
    return int(os.getenv('ELASTICSEARCH_PORT', '9200'))


def get_environment_config() -> Dict[str, Any]:
    """
    Get complete environment configuration.
    """
    return {
        'kafka': {
            'bootstrap_servers': get_kafka_bootstrap_servers()
        },
        'minio': {
            'endpoint': get_minio_endpoint(),
            'access_key': get_minio_credentials()[0],
            'secret_key': get_minio_credentials()[1]
        },
        'elasticsearch': {
            'host': get_elasticsearch_host(),
            'port': get_elasticsearch_port()
        },
        'environment': {
            'in_container': os.path.exists('/.dockerenv'),
            'is_production': os.getenv('ENVIRONMENT', 'development') == 'production'
        }
    }


def print_environment_info():
    """
    Print current environment configuration for debugging.
    """
    config = get_environment_config()
    print("Environment Configuration:")
    print(f"  Container: {config['environment']['in_container']}")
    print(f"  Production: {config['environment']['is_production']}")
    print(f"  Kafka: {config['kafka']['bootstrap_servers']}")
    print(f"  MinIO: {config['minio']['endpoint']}")
    print(f"  Elasticsearch: {config['elasticsearch']['host']}:{config['elasticsearch']['port']}")


if __name__ == "__main__":
    print_environment_info()
