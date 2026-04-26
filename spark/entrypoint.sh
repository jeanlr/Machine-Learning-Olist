#!/bin/bash
# spark/entrypoint.sh

# Set AWS credentials for MinIO
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}

# Start Spark based on mode
if [ "$SPARK_MODE" = "master" ]; then
    echo "Starting Spark Master"
    /opt/spark/sbin/start-master.sh
    tail -f /opt/spark/logs/spark-*.out
elif [ "$SPARK_MODE" = "worker" ]; then
    echo "Starting Spark Worker connecting to $SPARK_MASTER_URL"
    /opt/spark/sbin/start-worker.sh $SPARK_MASTER_URL
    tail -f /opt/spark/logs/spark-*.out
else
    echo "Unknown SPARK_MODE: $SPARK_MODE"
    exit 1
fi