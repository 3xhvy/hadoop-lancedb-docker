#!/bin/bash

# Start SSH service
service ssh start

# Format namenode if it doesn't exist
if [ ! -d "$HADOOP_HOME/data/namenode/current" ]; then
    echo "Formatting namenode..."
    hdfs namenode -format
fi

# Start HDFS
echo "Starting HDFS..."
start-dfs.sh

# Start YARN
echo "Starting YARN..."
start-yarn.sh

# Wait for services to be up
sleep 5

# Create user directories in HDFS
echo "Creating user directories in HDFS..."
hdfs dfs -mkdir -p /user/root
hdfs dfs -chown root:root /user/root

# Create LanceDB directory in HDFS
echo "Creating LanceDB directory in HDFS..."
hdfs dfs -mkdir -p /lancedb
hdfs dfs -chown root:root /lancedb

echo "Hadoop and HDFS are running!"
echo "HDFS UI: http://localhost:9870"
echo "YARN UI: http://localhost:8088"

# Keep container running
tail -f /dev/null
