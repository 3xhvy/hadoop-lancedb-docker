#!/bin/bash

# Start SSH service and ensure it's running
echo "Starting SSH service..."
service ssh start
status=$?
if [ $status -ne 0 ]; then
    echo "Failed to start SSH service"
    exit 1
fi

# Verify SSH connectivity to localhost
echo "Verifying SSH connectivity..."
ssh -o StrictHostKeyChecking=no localhost echo "SSH connection successful"
if [ $? -ne 0 ]; then
    echo "SSH connectivity test failed. Attempting to fix..."
    mkdir -p /root/.ssh
    ssh-keygen -t rsa -P '' -f /root/.ssh/id_rsa
    cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys
    chmod 700 /root/.ssh
    chmod 600 /root/.ssh/authorized_keys
    echo "Host localhost" > /root/.ssh/config
    echo "  StrictHostKeyChecking no" >> /root/.ssh/config
    echo "  UserKnownHostsFile=/dev/null" >> /root/.ssh/config
    chmod 600 /root/.ssh/config
    
    # Test again
    ssh -o StrictHostKeyChecking=no localhost echo "SSH connection successful"
    if [ $? -ne 0 ]; then
        echo "SSH connectivity test still failed. Hadoop services may not start properly."
    else
        echo "SSH connectivity fixed successfully."
    fi
fi

# Format namenode if it doesn't exist
if [ ! -d "$HADOOP_HOME/data/namenode/current" ]; then
    echo "Formatting namenode..."
    hdfs namenode -format
fi

# Start HDFS
echo "Starting HDFS..."
start-dfs.sh
if [ $? -ne 0 ]; then
    echo "Failed to start HDFS. Check SSH configuration."
    exit 1
fi

# Start YARN
echo "Starting YARN..."
start-yarn.sh

# Wait for services to be up
sleep 5

# Check if services are running
echo "Checking Hadoop services..."
jps

# Create user directories in HDFS
echo "Creating user directories in HDFS..."
hdfs dfs -mkdir -p /user/root
hdfs dfs -chown root:root /user/root

# Create LanceDB directory in HDFS
echo "Creating LanceDB directory in HDFS..."
hdfs dfs -mkdir -p /lancedb_data
hdfs dfs -chown root:root /lancedb_data

echo "Hadoop and HDFS are running!"
echo "HDFS UI: http://localhost:9870"
echo "YARN UI: http://localhost:8088"

# Keep container running
tail -f /dev/null
