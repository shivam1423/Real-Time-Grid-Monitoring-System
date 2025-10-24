#!/bin/bash

# Energy Grid Pulse - Stop Complete Pipeline
# Stops all running processes and optionally stops Docker containers

echo "=========================================="
echo "Stopping Energy Grid Pipeline"
echo "=========================================="
echo ""

# Stop Python processes using PIDs
if [ -f "logs/data_generator.pid" ]; then
    PID=$(cat logs/data_generator.pid)
    echo "Stopping data generator (PID: $PID)..."
    kill $PID 2>/dev/null && echo "   Stopped" || echo "   Already stopped"
    rm -f logs/data_generator.pid
fi

if [ -f "logs/spark_to_es.pid" ]; then
    PID=$(cat logs/spark_to_es.pid)
    echo "Stopping Spark to Elasticsearch (PID: $PID)..."
    kill $PID 2>/dev/null && echo "   Stopped" || echo "   Already stopped"
    rm -f logs/spark_to_es.pid
fi

if [ -f "logs/spark_process.pid" ]; then
    PID=$(cat logs/spark_process.pid)
    echo "Stopping Spark processor (PID: $PID)..."
    kill $PID 2>/dev/null && echo "   Stopped" || echo "   Already stopped"
    rm -f logs/spark_process.pid
fi

# Backup: Kill any remaining processes
echo ""
echo "Cleaning up any remaining processes..."
pkill -f "energy_grid_generator.py" 2>/dev/null && echo "   Killed remaining data generators" || true
pkill -f "spark_to_elasticsearch.py" 2>/dev/null && echo "   Killed remaining Spark ES writers" || true
pkill -f "process_energy_grid.py" 2>/dev/null && echo "   Killed remaining Spark processors" || true

echo ""
echo "Do you want to stop Docker containers too? (y/N)"
read -t 5 -n 1 STOP_DOCKER || STOP_DOCKER="n"
echo ""

if [[ $STOP_DOCKER =~ ^[Yy]$ ]]; then
    echo "Stopping Docker containers..."
    docker-compose down
    echo "   Docker containers stopped"
else
    echo "Docker containers are still running"
    echo "   To stop them later, run: docker-compose down"
fi

echo ""
echo "=========================================="
echo "Pipeline stopped"
echo "=========================================="
echo ""

