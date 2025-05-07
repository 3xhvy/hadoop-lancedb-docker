#!/bin/bash

# Setup SSH for passwordless login
echo "Setting up SSH..."
mkdir -p ~/.ssh
echo "Host *
  UserKnownHostsFile /dev/null
  StrictHostKeyChecking no" > ~/.ssh/config
chmod 600 ~/.ssh/config

# Generate SSH key if not exists
if [ ! -f ~/.ssh/id_rsa ]; then
    ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    chmod 600 ~/.ssh/authorized_keys
fi

# Start SSH service
echo "Starting SSH service..."
service ssh start

# Wait for SSH to be ready
echo "Waiting for SSH to be ready..."
sleep 2

# Test SSH connection
echo "Testing SSH connection..."
ssh -o StrictHostKeyChecking=no localhost "echo SSH connection successful"

# Format namenode if not already formatted
if [ ! -d "/opt/hadoop/data/namenode/current" ]; then
    echo "Formatting namenode..."
    hdfs namenode -format
fi

# Start Hadoop services
echo "Starting Hadoop services..."
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

# Wait for Hadoop to be ready
echo "Waiting for Hadoop to be ready..."
sleep 5

# Create HDFS directories if they don't exist
echo "Creating HDFS directories..."
hdfs dfs -mkdir -p /hadoop_lancedb_data
hdfs dfs -chmod -R 777 /hadoop_lancedb_data

# Start Spark standalone cluster
echo "Starting Spark standalone cluster..."
HOSTNAME_FQDN=$(hostname -f)
$SPARK_HOME/sbin/start-master.sh --host $HOSTNAME_FQDN
sleep 5  # Wait for master to be ready
$SPARK_HOME/sbin/start-worker.sh spark://$HOSTNAME_FQDN:7077

# Print service status
echo "Checking service status..."
jps

echo "All services started. Container is ready."

# Keep container running and show logs
tail -f $SPARK_HOME/logs/* $HADOOP_HOME/logs/* /dev/null
