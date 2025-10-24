#!/bin/bash

# Energy Grid Pulse - Complete Pipeline Startup
# This script starts EVERYTHING needed to see the dashboard
# Just run: ./run_complete_pipeline.sh

set -e

echo "=========================================="
echo "Energy Grid Pulse - Complete Pipeline"
echo "=========================================="
echo ""

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Virtual environment not found. Creating one..."
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
else
    source venv/bin/activate
fi

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Docker is not running. Please start Docker Desktop and try again."
    exit 1
fi

echo "Docker is running"
echo ""

# Start Docker services
echo "Starting Docker services..."
docker-compose up -d

echo "Waiting for services to initialize (60 seconds)..."
sleep 60

# Create Kafka topics
echo ""
echo "Creating Kafka topics..."
python kafka_admin_client/create_topics.py || echo "Topics may already exist"

# Start data generator in background
echo ""
echo "Starting data generator..."
nohup python data_generator/energy_grid_generator.py config/energy_grid_config.yaml > logs/data_generator.log 2>&1 &
DATA_GEN_PID=$!
echo "   PID: $DATA_GEN_PID"

# Wait a moment for data to start flowing
sleep 5

# Start Spark to Elasticsearch writer
echo ""
echo "Starting Spark to Elasticsearch writer..."
nohup python spark_scripts/spark_to_elasticsearch.py > logs/spark_to_es.log 2>&1 &
SPARK_ES_PID=$!
echo "   PID: $SPARK_ES_PID"

# Start other Spark processors (optional)
echo ""
echo "Starting additional Spark processors..."
nohup python spark_scripts/process_energy_grid.py > logs/process_grid.log 2>&1 &
SPARK_PROCESS_PID=$!
echo "   PID: $SPARK_PROCESS_PID"

# Save PIDs for stopping later
mkdir -p logs
echo "$DATA_GEN_PID" > logs/data_generator.pid
echo "$SPARK_ES_PID" > logs/spark_to_es.pid
echo "$SPARK_PROCESS_PID" > logs/spark_process.pid

echo ""
echo "=========================================="
echo "COMPLETE PIPELINE IS RUNNING"
echo "=========================================="
echo ""
echo "Access your dashboards:"
echo "   Dashboard URL: http://localhost:5601/app/dashboards#/view/energy-grid-pro-dashboard"
echo ""
echo "Other UIs:"
echo "   Kibana:  http://localhost:5601"
echo "   Airflow: http://localhost:8082"
echo "   Spark:   http://localhost:8080"
echo "   MinIO:   http://localhost:9001"
echo ""
echo "Logs are in: logs/"
echo "   - data_generator.log"
echo "   - spark_to_es.log"
echo "   - process_grid.log"
echo ""
echo "To stop everything, run:"
echo "   ./stop_complete_pipeline.sh"
echo ""
echo "Dashboard should have data in 30-60 seconds..."
echo "   (Refresh the page if it's empty initially)"
echo ""

