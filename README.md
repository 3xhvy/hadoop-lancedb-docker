# Hadoop with LanceDB Docker Container

This Docker setup provides a Hadoop environment with Python and LanceDB integration.

## Features

- Hadoop 3.3.6 with HDFS and YARN
- Python 3.10 environment
- LanceDB for vector database functionality
- Shared volume for Python applications

## Getting Started

### Prerequisites

- Docker and Docker Compose installed on your system

### Building and Running

1. Build and start the container:

```bash
docker-compose up -d
```

2. Check if the container is running:

```bash
docker ps
```

3. Access the Hadoop web interfaces:
   - HDFS NameNode UI: http://localhost:9870
   - YARN ResourceManager UI: http://localhost:8088

### Using the Container

#### Running Python Scripts

Your Python scripts in the `python_apps` directory are mounted inside the container. To run a script:

```bash
docker exec -it hadoop-lancedb python3.10 /home/hadoop/python_apps/lancedb_example.py
```

#### Accessing the Container Shell

```bash
docker exec -it hadoop-lancedb bash
```

#### Using HDFS Commands

```bash
docker exec -it hadoop-lancedb hdfs dfs -ls /
```

## LanceDB with Hadoop

The container includes LanceDB, which can be used with local storage or HDFS:

- Local storage path: `/opt/lancedb_data`
- HDFS path: `hdfs://localhost:9000/lancedb`

Example Python code to connect to LanceDB using HDFS:

```python
import lancedb

# Connect using HDFS
db = lancedb.connect("hdfs://localhost:9000/lancedb/my_database")
```

## Data Persistence

The following volumes are configured for data persistence:

- `hadoop_data`: Stores HDFS data
- `lancedb_data`: Stores LanceDB data
- `./python_apps`: Mounted to `/home/hadoop/python_apps` for your Python code

## Example Application

An example Python script is provided in `python_apps/lancedb_example.py` that demonstrates basic LanceDB functionality.
