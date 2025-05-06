#!/usr/bin/env python3
"""
Example script demonstrating how to load data from Hadoop to LanceDB using Hadoop Streaming
This approach is useful for processing very large datasets that don't fit in memory
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

# Hadoop Streaming Mapper
def mapper():
    """
    Mapper function for Hadoop Streaming
    Reads input lines from stdin, processes them, and outputs key-value pairs
    
    In a real Hadoop Streaming job, this would be a separate script (mapper.py)
    that's submitted to the Hadoop job
    """
    print("This is a simulation of a Hadoop Streaming mapper")
    print("In a real scenario, this would run as a separate script on each Hadoop node")
    
    # In Hadoop Streaming, the mapper reads from stdin line by line
    # Here we'll simulate some input data
    sample_data = []
    for i in range(1, 501):  # Generate 500 sample records
        vector = [float(np.random.rand()) for _ in range(16)]  # 16-dim vector
        record = {
            "id": i,
            "vector": vector,
            "metadata": f"item{i}"
        }
        sample_data.append(record)
        
        # In a real mapper, we'd output key-value pairs to stdout
        # key = record["id"]
        # value = json.dumps(record)
        # print(f"{key}\t{value}")
    
    return sample_data

# Hadoop Streaming Reducer
def reducer(mapper_output):
    """
    Reducer function for Hadoop Streaming
    Reads key-value pairs from stdin, aggregates them, and outputs results
    
    In a real Hadoop Streaming job, this would be a separate script (reducer.py)
    that's submitted to the Hadoop job
    """
    print("This is a simulation of a Hadoop Streaming reducer")
    print("In a real scenario, this would run as a separate script on each Hadoop node")
    
    # In a real reducer, we'd read from stdin and process the data
    # Here we'll just use the mapper output directly
    return mapper_output

def main():
    print("Loading data from Hadoop to LanceDB using Hadoop Streaming")
    
    # Step 1: Simulate a Hadoop Streaming job
    print("Simulating Hadoop Streaming job...")
    
    # In a real scenario, you would run a Hadoop Streaming job like:
    # hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    #   -files mapper.py,reducer.py \
    #   -mapper "python mapper.py" \
    #   -reducer "python reducer.py" \
    #   -input /input/data \
    #   -output /output/data
    
    # For this example, we'll simulate the mapper and reducer
    mapper_output = mapper()
    reducer_output = reducer(mapper_output)
    
    print(f"Hadoop Streaming job completed with {len(reducer_output)} records")
    
    # Step 2: Write the output to HDFS
    print("Writing output to HDFS...")
    
    try:
        # Connect to HDFS
        hdfs = fs.HadoopFileSystem(host="localhost", port=9000)
        
        # Create output directory
        hdfs_output_path = "/hadoop_streaming_output"
        try:
            hdfs.create_dir(hdfs_output_path)
            print(f"Created directory {hdfs_output_path} in HDFS")
        except:
            print(f"Directory {hdfs_output_path} already exists in HDFS")
        
        # Convert reducer output to a DataFrame
        df = pd.DataFrame(reducer_output)
        
        # Convert to PyArrow Table
        table = pa.Table.from_pandas(df)
        
        # Write to HDFS as Parquet
        parquet_path = f"{hdfs_output_path}/streaming_output.parquet"
        pq.write_table(table, parquet_path, filesystem=hdfs)
        print(f"Successfully wrote data to HDFS at {parquet_path}")
        
        # Step 3: Read the data from HDFS
        print(f"Reading data from HDFS at {parquet_path}")
        hdfs_table = pq.read_table(parquet_path, filesystem=hdfs)
        hdfs_df = hdfs_table.to_pandas()
        print(f"Successfully read {len(hdfs_df)} rows from HDFS")
        
        # Step 4: Load the data into LanceDB
        print("Loading data into LanceDB...")
        local_db_path = "/opt/lancedb_data/streaming_example_db"
        db = lancedb.connect(local_db_path)
        
        # Create or overwrite the table
        table = db.create_table("streaming_vectors", data=hdfs_df, mode="overwrite")
        print(f"Created LanceDB table 'streaming_vectors' with {len(hdfs_df)} rows")
        
        # Step 5: Create a vector index for similarity search
        print("Creating vector index...")
        table.create_index(
            metric="cosine",
            vector_column_name="vector",
            wait_timeout=timedelta(seconds=60)
        )
        print("Vector index created successfully")
        
        # Step 6: Perform a vector search
        query_vector = [0.5] * 16  # Sample query vector
        print("Performing vector search...")
        results = table.search(query_vector).limit(5).to_pandas()
        
        print("\nSearch Results:")
        print(results)
        
    except Exception as e:
        print(f"Error: {e}")
        print("Falling back to local processing...")
        
        # Process locally if HDFS is not available
        df = pd.DataFrame(reducer_output)
        
        # Load into LanceDB
        local_db_path = "/opt/lancedb_data/streaming_example_db"
        db = lancedb.connect(local_db_path)
        table = db.create_table("streaming_vectors", data=df, mode="overwrite")
        
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
Hadoop Streaming to LanceDB Integration:
---------------------------------------
1. This example demonstrates using Hadoop Streaming to process data for LanceDB
2. Benefits of this approach:
   - Works with any programming language (Python, Ruby, etc.)
   - Simple to implement for data transformations
   - Good for ETL pipelines feeding into LanceDB
3. The workflow is:
   - Process data with Hadoop Streaming (mapper/reducer)
   - Write output to HDFS
   - Read from HDFS using PyArrow
   - Load into LanceDB for vector search
4. For production use:
   - Implement proper error handling
   - Consider chunking large datasets
   - Use compression for HDFS storage
""")

if __name__ == "__main__":
    main()
