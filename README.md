# Hadoop with LanceDB Integration

This Docker setup provides a Hadoop environment with Python and LanceDB integration, demonstrating various methods to integrate Hadoop's distributed processing with LanceDB's vector search capabilities.

## Features

- Hadoop 3.3.6 with HDFS and YARN
- Python 3.10 environment
- LanceDB for vector database functionality
- Multiple integration methods (PyArrow, PySpark, Hadoop Streaming, MapReduce)
- Shared volume for Python applications

## Getting Started

### Prerequisites

- Docker and Docker Compose installed on your system

### Building and Running

1. Clone this repository:

```bash
git clone <repository-url>
cd hadoop-lancedb
```

2. Build and start the container:

```bash
docker-compose up -d
```

3. Check if the container is running:

```bash
docker ps
```

4. Access the Hadoop web interfaces:
   - HDFS NameNode UI: http://localhost:9870
   - YARN ResourceManager UI: http://localhost:8088

### Installing Dependencies

The Docker container comes with most dependencies pre-installed, but if you need additional packages:

```bash
docker exec -it custom-hadoop pip install <package-name>
```

For PySpark integration, you may need to install:

```bash
docker exec -it custom-hadoop pip install pyspark
```

## Integration Methods in Detail

This project demonstrates four different methods to integrate Hadoop with LanceDB, each with its own strengths and use cases.

### 1. PyArrow Integration (`lancedb_example.py`)

**Overview**: The simplest approach using PyArrow's HDFS support to read/write data between Hadoop and LanceDB.

**Technical Details**:
- Uses PyArrow's `HadoopFileSystem` class to connect to HDFS
- Reads/writes data in Parquet format, which is efficient for columnar data
- Direct path from HDFS to LanceDB without complex transformations

**Code Pattern**:
```python
# Connect to HDFS via PyArrow
hdfs = fs.HadoopFileSystem(host="localhost", port=9000)

# Read/write data to HDFS
hdfs_path = "/path/in/hdfs"
pq.write_table(table, f"{hdfs_path}/data.parquet", filesystem=hdfs)
hdfs_table = pq.read_table(f"{hdfs_path}/data.parquet", filesystem=hdfs)

# Convert to pandas and load into LanceDB
df = hdfs_table.to_pandas()
db = lancedb.connect("/path/to/db")
table = db.create_table("table_name", data=df)
```

**Best For**:
- Simple data transfer between HDFS and LanceDB
- Smaller datasets that fit in memory
- When minimal data transformation is needed

**Run Example**:
```bash
docker exec -it custom-hadoop python3.10 /home/hadoop/python_apps/lancedb_example.py
```

### 2. PySpark Integration (`spark_hadoop_lancedb.py`)

**Overview**: Uses Spark to process data in Hadoop before loading into LanceDB, leveraging Spark's distributed computing capabilities.

**Technical Details**:
- Utilizes Spark SQL for data transformations and filtering
- Can process data larger than memory using Spark's distributed processing
- Supports complex transformations before loading into LanceDB

**Code Pattern**:
```python
# Initialize Spark with Hadoop configuration
spark = SparkSession.builder \
    .appName("Hadoop-LanceDB-Integration") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# Read/process data with Spark
spark_df = spark.read.parquet("hdfs:///path/to/data")
filtered_df = spark_df.filter(col("vector")[0] > 0.5)

# Convert to pandas and load into LanceDB
pandas_df = filtered_df.toPandas()
db = lancedb.connect("/path/to/db")
table = db.create_table("table_name", data=pandas_df)
```

**Best For**:
- Large-scale data processing before vector search
- Complex SQL-like transformations and filtering
- When you need to leverage Spark's ecosystem (ML, streaming, etc.)

**Run Example**:
```bash
docker exec -it custom-hadoop python3.10 /home/hadoop/python_apps/spark_hadoop_lancedb.py
```

### 3. Hadoop Streaming (`hadoop_streaming_lancedb.py`)

**Overview**: Uses Hadoop Streaming to process data with any programming language before loading into LanceDB.

**Technical Details**:
- Hadoop Streaming allows processing with any language that can read from stdin and write to stdout
- Mapper and reducer scripts process data line by line
- Flexible approach that works with Python, Ruby, Perl, etc.

**Code Pattern**:
```python
# In a real Hadoop Streaming job, you'd run:
# hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
#   -files mapper.py,reducer.py \
#   -mapper "python mapper.py" \
#   -reducer "python reducer.py" \
#   -input /input/data \
#   -output /output/data

# Mapper function (would be in separate file)
def mapper():
    for line in sys.stdin:
        # Process input line
        key, value = process_line(line)
        print(f"{key}\t{value}")

# Reducer function (would be in separate file)
def reducer():
    for line in sys.stdin:
        # Process key-value pairs
        key, value = line.strip().split('\t')
        # Aggregate and output results
        
# Read output from Hadoop Streaming job
hdfs = fs.HadoopFileSystem(host="localhost", port=9000)
hdfs_table = pq.read_table("/hadoop/output/path", filesystem=hdfs)
df = hdfs_table.to_pandas()

# Load into LanceDB
db = lancedb.connect("/path/to/db")
table = db.create_table("table_name", data=df)
```

**Best For**:
- When you need to use languages other than Java/Scala
- Simple data transformations with line-by-line processing
- Integrating with existing scripts in various languages

**Run Example**:
```bash
docker exec -it custom-hadoop python3.10 /home/hadoop/python_apps/hadoop_streaming_lancedb.py
```

### 4. Hadoop MapReduce (`hadoop_mapreduce_lancedb.py`)

**Overview**: The most scalable approach using Hadoop's MapReduce paradigm for processing massive datasets before loading into LanceDB.

**Technical Details**:
- Based on the MapReduce programming model for parallel processing
- Highly fault-tolerant and scalable across large clusters
- Processes data in batches, ideal for datasets that don't fit in memory

**Code Pattern**:
```python
# In a real MapReduce job, you'd implement Mapper and Reducer classes
class Mapper:
    def map(self, key, value):
        # Process input and emit key-value pairs
        return [(key, processed_value)]

class Reducer:
    def reduce(self, key, values):
        # Process values with the same key
        return [aggregated_result]

# Process MapReduce output in batches
hdfs = fs.HadoopFileSystem(host="localhost", port=9000)
file_infos = hdfs.get_file_info(fs.FileSelector("/mapreduce/output"))
parquet_files = [f.path for f in file_infos if f.is_file]

# Create LanceDB table with schema only
db = lancedb.connect("/path/to/db")
table = db.create_table("table_name", schema=schema)

# Load data in batches
for parquet_file in parquet_files:
    batch_table = pq.read_table(parquet_file, filesystem=hdfs)
    batch_df = batch_table.to_pandas()
    table.add(batch_df)
```

**Best For**:
- Processing extremely large datasets (terabytes+)
- When fault tolerance is critical
- Complex data transformations that benefit from the MapReduce paradigm

**Run Example**:
```bash
docker exec -it custom-hadoop python3.10 /home/hadoop/python_apps/hadoop_mapreduce_lancedb.py
```

## Integration Architecture Comparison

| Feature | PyArrow | PySpark | Hadoop Streaming | MapReduce |
|---------|---------|---------|-----------------|-----------|
| **Complexity** | Low | Medium | Medium | High |
| **Scalability** | Medium | High | Medium | Very High |
| **Memory Efficiency** | Low | High | Medium | High |
| **Processing Power** | Low | High | Medium | High |
| **Language Support** | Python | Python, Scala, Java | Any | Java, Python* |
| **Learning Curve** | Gentle | Moderate | Moderate | Steep |
| **Best Dataset Size** | Small-Medium | Medium-Large | Medium | Large-Massive |

*Python support via libraries like mrjob

## Integration Workflow Patterns

All four integration methods follow these general steps, with variations in implementation:

1. **Data Storage**: Store raw data in HDFS
2. **Data Processing**: Process data using one of the four methods
3. **Intermediate Storage**: Store processed data in HDFS (usually Parquet format)
4. **Data Loading**: Load processed data into LanceDB
5. **Vector Indexing**: Build vector indexes in LanceDB
6. **Vector Search**: Perform similarity searches

## Using the Container

### Running Python Scripts

Your Python scripts in the `python_apps` directory are mounted inside the container. To run a script:

```bash
docker exec -it custom-hadoop python3.10 /home/hadoop/python_apps/lancedb_example.py
```

### Accessing the Container Shell

```bash
docker exec -it custom-hadoop bash
```

### Using HDFS Commands

```bash
docker exec -it custom-hadoop hdfs dfs -ls /
```

## LanceDB with Hadoop

The container includes LanceDB, which can be used with local storage or HDFS via PyArrow:

- Local storage path: `/opt/lancedb_data`
- HDFS path for PyArrow: `hdfs://localhost:9000/lancedb_data`

Example Python code to connect to LanceDB using local storage:

```python
import lancedb

# Connect using local storage
db = lancedb.connect("/opt/lancedb_data/my_database")
```

Example using PyArrow to read from HDFS and load into LanceDB:

```python
import pyarrow.fs as fs
import pyarrow.parquet as pq
import lancedb

# Connect to HDFS
hdfs = fs.HadoopFileSystem(host="localhost", port=9000)

# Read data from HDFS
hdfs_path = "/lancedb_data/vectors.parquet"
hdfs_table = pq.read_table(hdfs_path, filesystem=hdfs)
hdfs_df = hdfs_table.to_pandas()

# Load into LanceDB
db = lancedb.connect("/opt/lancedb_data/my_database")
table = db.create_table("my_table", data=hdfs_df, mode="overwrite")
```

## Data Persistence

The following volumes are configured for data persistence:

- `hadoop_data`: Stores HDFS data
- `lancedb_data`: Stores LanceDB data
- `./python_apps`: Mounted to `/home/hadoop/python_apps` for your Python code

## Troubleshooting

### HDFS Connection Issues

If you encounter HDFS connection issues:

1. Verify SSH is properly configured:
```bash
docker exec -it custom-hadoop ssh localhost echo "SSH works"
```

2. Check if Hadoop services are running:
```bash
docker exec -it custom-hadoop jps
```

3. Restart Hadoop services if needed:
```bash
docker exec -it custom-hadoop stop-all.sh
docker exec -it custom-hadoop start-dfs.sh
docker exec -it custom-hadoop start-yarn.sh
```

### LanceDB Issues

If you encounter issues with LanceDB:

1. Ensure the vector dimensions are divisible by 8 or 16 to avoid PQ performance warnings
2. For large datasets, process in batches to avoid memory issues
3. Use `wait_timeout` parameter when creating indexes on large tables

## References

- [Hadoop Documentation](https://hadoop.apache.org/docs/r3.3.6/)
- [LanceDB Documentation](https://lancedb.github.io/lancedb/)
- [PyArrow Documentation](https://arrow.apache.org/docs/python/)
