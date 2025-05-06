# Base image
FROM ubuntu:20.04

# Avoid interactive timezone prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install prerequisites and add deadsnakes PPA for Python 3.10
RUN apt update && \
    apt install -y software-properties-common curl wget gnupg net-tools openssh-server openssh-client && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt update && \
    apt install -y openjdk-8-jdk python3.10 python3.10-venv python3.10-dev python3-pip build-essential python3.10-distutils && \
    apt clean

# Set JAVA_HOME (required by Hadoop)
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Install Hadoop
ENV HADOOP_VERSION=3.3.6
RUN wget https://downloads.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz && \
    tar -xzf hadoop-$HADOOP_VERSION.tar.gz -C /opt/ && \
    rm hadoop-$HADOOP_VERSION.tar.gz && \
    ln -s /opt/hadoop-$HADOOP_VERSION /opt/hadoop

# Set Hadoop environment variables
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Configure SSH for Hadoop - IMPROVED SETUP
RUN mkdir -p /var/run/sshd && \
    echo "PasswordAuthentication yes" >> /etc/ssh/sshd_config && \
    echo "PermitRootLogin yes" >> /etc/ssh/sshd_config && \
    # Create SSH directory and set permissions
    mkdir -p /root/.ssh && \
    # Generate SSH key pair
    ssh-keygen -t rsa -P '' -f /root/.ssh/id_rsa && \
    # Add public key to authorized_keys
    cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys && \
    # Set proper permissions
    chmod 700 /root/.ssh && \
    chmod 600 /root/.ssh/authorized_keys && \
    # Configure SSH to not check host keys for localhost
    echo "Host localhost" > /root/.ssh/config && \
    echo "  StrictHostKeyChecking no" >> /root/.ssh/config && \
    echo "  UserKnownHostsFile=/dev/null" >> /root/.ssh/config && \
    chmod 600 /root/.ssh/config

# Configure Hadoop for standalone operation
RUN mkdir -p $HADOOP_HOME/data/namenode $HADOOP_HOME/data/datanode && \
    echo "export JAVA_HOME=${JAVA_HOME}" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_NAMENODE_USER=root" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_DATANODE_USER=root" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_SECONDARYNAMENODE_USER=root" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
    echo "export YARN_RESOURCEMANAGER_USER=root" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
    echo "export YARN_NODEMANAGER_USER=root" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# Install pip for Python 3.10 and required packages
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10 && \
    python3.10 -m pip install --no-cache-dir lancedb pyarrow pandas numpy

# Create a directory for LanceDB data
RUN mkdir -p /opt/lancedb_data

# Create Python apps directory
RUN mkdir -p /home/hadoop/python_apps

# Copy configuration files and initialization script
COPY core-site.xml $HADOOP_HOME/etc/hadoop/
COPY hdfs-site.xml $HADOOP_HOME/etc/hadoop/
COPY init-hadoop.sh /root/init-hadoop.sh
RUN chmod +x /root/init-hadoop.sh

# Set Python path
ENV PYTHONPATH=/home/hadoop/python_apps:$PYTHONPATH

# Expose Hadoop ports
EXPOSE 9870 9864 9000 8088

# Default command
CMD ["/root/init-hadoop.sh"]
