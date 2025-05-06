#!/usr/bin/env python3
"""
Example script demonstrating how to load data from Hadoop to LanceDB using Hadoop MapReduce
This approach is useful for complex data processing before loading into LanceDB
"""
import os
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq
import lancedb
import json
import sys
from datetime import timedelta

class VectorMapper:
    """
    Mapper class for Hadoop MapReduce
    
    In a real Hadoop MapReduce job using mrjob or Hadoop Java API, 
    this would be implemented as a proper Mapper class
    """
    def __init__(self):
        self.vector_dim = 16
    
    def map(self, key, value):
        """
        Map function that processes input records
        
        Args:
            key: Record key (e.g., line number)
            value: Record value (e.g., input line)
            
        Returns:
            List of (key, value) pairs
        """
        # In a real mapper, value would be the input record
        # Here we'll generate a random vector for demonstration
        vector = [float(np.random.rand()) for _ in range(self.vector_dim)]
        
        # Create a record with id, vector, and metadata
        record = {
            "id": key,
            "vector": vector,
            "metadata": f"item{key}",
            "partition": key % 10  # For partitioning in the reducer
        }
        
        # Emit the record with partition as key for the reducer
        return [(record["partition"], record)]

class VectorReducer:
    """
    Reducer class for Hadoop MapReduce
    
    In a real Hadoop MapReduce job using mrjob or Hadoop Java API,
    this would be implemented as a proper Reducer class
    """
    def reduce(self, key, values):
        """
        Reduce function that processes mapper output
        
        Args:
            key: Key from mapper (partition in this case)
            values: List of values from mapper with the same key
            
        Returns:
            List of output records
        """
        # In a real reducer, we might aggregate or process the values
        # Here we'll just pass them through
        return list(values)

def simulate_mapreduce_job(num_records=500):
    """
    Simulate a Hadoop MapReduce job
    
    In a real scenario, this would be a Hadoop MapReduce job submitted to the cluster
    
    Args:
        num_records: Number of records to generate
        
    Returns:
        List of output records from the reducer
    """
    print(f"Simulating MapReduce job with {num_records} records...")
    
    # Create mapper and reducer instances
    mapper = VectorMapper()
    reducer = VectorReducer()
    
    # Simulate map phase
    map_output = []
    for i in range(1, num_records + 1):
        map_output.extend(mapper.map(i, None))
    
    # Group map output by key (partition)
    grouped_output = {}
    for key, value in map_output:
        if key not in grouped_output:
            grouped_output[key] = []
        grouped_output[key].append(value)
    
    # Simulate reduce phase
    reduce_output = []
    for key, values in grouped_output.items():
        reduce_output.extend(reducer.reduce(key, values))
    
    print(f"MapReduce job completed with {len(reduce_output)} records")
    return reduce_output

def process_in_batches(data, batch_size=100):
    """
    Process data in batches to handle large datasets
    
    Args:
        data: List of records to process
        batch_size: Number of records per batch
        
    Yields:
        Batches of records
    """
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]

def main():
    print("Loading data from Hadoop to LanceDB using MapReduce")
    
    # Step 1: Simulate a Hadoop MapReduce job
    mapreduce_output = simulate_mapreduce_job(500)
    
    # Step 2: Write the output to HDFS
    print("Writing output to HDFS...")
    
    try:
        # Connect to HDFS
        hdfs = fs.HadoopFileSystem(host="localhost", port=9000)
        
        # Create output directory
        hdfs_output_path = "/hadoop_mapreduce_output"
        try:
            hdfs.create_dir(hdfs_output_path)
            print(f"Created directory {hdfs_output_path} in HDFS")
        except:
            print(f"Directory {hdfs_output_path} already exists in HDFS")
        
        # Process data in batches (simulating multiple reducer outputs)
        batch_number = 0
        for batch in process_in_batches(mapreduce_output, 100):
            batch_number += 1
            
            # Convert batch to DataFrame
            df = pd.DataFrame(batch)
            
            # Convert to PyArrow Table
            table = pa.Table.from_pandas(df)
            
            # Write batch to HDFS as Parquet
            parquet_path = f"{hdfs_output_path}/part-{batch_number:05d}.parquet"
            pq.write_table(table, parquet_path, filesystem=hdfs)
            print(f"Wrote batch {batch_number} to HDFS at {parquet_path}")
        
        # Step 3: Connect to LanceDB
        print("Connecting to LanceDB...")
        local_db_path = "/opt/lancedb_data/mapreduce_example_db"
        db = lancedb.connect(local_db_path)
        
        # Step 4: Create a table schema (without data)
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("vector", pa.list_(pa.float64(), 16)),  # 16-dim vector
            pa.field("metadata", pa.string()),
            pa.field("partition", pa.int64())
        ])
        
        # Create the table with schema only
        table_name = "mapreduce_vectors"
        table = db.create_table(table_name, schema=schema, mode="overwrite")
        print(f"Created LanceDB table '{table_name}' with schema")
        
        # Step 5: Read and load data from HDFS in batches
        print("Loading data from HDFS to LanceDB in batches...")
        
        # List all files in the output directory
        file_infos = hdfs.get_file_info(fs.FileSelector(hdfs_output_path))
        parquet_files = [f.path for f in file_infos if f.is_file and f.path.endswith(".parquet")]
        
        total_rows = 0
        for parquet_file in parquet_files:
            # Read the Parquet file
            parquet_table = pq.read_table(parquet_file, filesystem=hdfs)
            parquet_df = parquet_table.to_pandas()
            
            # Add to LanceDB table
            table.add(parquet_df)
            
            total_rows += len(parquet_df)
            print(f"Added {len(parquet_df)} rows from {parquet_file} to LanceDB")
        
        print(f"Total rows loaded into LanceDB: {total_rows}")
        
        # Step 6: Create a vector index for similarity search
        print("Creating vector index...")
        table.create_index(
            metric="cosine",
            vector_column_name="vector",
            wait_timeout=timedelta(seconds=60)
        )
        print("Vector index created successfully")
        
        # Step 7: Perform a vector search
        query_vector = [0.5] * 16  # Sample query vector
        print("Performing vector search...")
        results = table.search(query_vector).limit(5).to_pandas()
        
        print("\nSearch Results:")
        print(results)
        
    except Exception as e:
        print(f"Error: {e}")
        print("Falling back to local processing...")
        
        # Process locally if HDFS is not available
        df = pd.DataFrame(mapreduce_output)
        
        # Load into LanceDB
        local_db_path = "/opt/lancedb_data/mapreduce_example_db"
        db = lancedb.connect(local_db_path)
        table = db.create_table("mapreduce_vectors", data=df, mode="overwrite")
        
        # Create index and search
        table.create_index(
            metric="cosine",
            vector_column_name="vector"
        )
        
        query_vector = [0.5] * 16
        results = table.search(query_vector).limit(5).to_pandas()
        
        print("\nSearch Results (from local processing):")
        print(results)
    
    print("""
Hadoop MapReduce to LanceDB Integration:
---------------------------------------
1. This example demonstrates using Hadoop MapReduce to process data for LanceDB
2. Benefits of this approach:
   - Highly scalable for processing massive datasets
   - Complex data transformations before loading into LanceDB
   - Fault tolerance through Hadoop's architecture
3. The workflow is:
   - Process data with MapReduce (map/reduce phases)
   - Write output to HDFS in partitioned files
   - Create LanceDB table with schema
   - Load data from HDFS in batches
   - Build vector index and perform searches
4. For production use:
   - Implement proper error handling
   - Use a real MapReduce framework (Hadoop Java API, mrjob, etc.)
   - Consider data partitioning strategies
   - Optimize batch sizes based on memory constraints
""")

if __name__ == "__main__":
    main()
