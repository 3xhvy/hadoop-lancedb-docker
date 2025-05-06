#!/usr/bin/env python3
"""
Example script demonstrating LanceDB usage with Hadoop via PyArrow
"""
import os
import pandas as pd
import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq
import lancedb
import numpy as np

def main():
    print("LanceDB with Hadoop Example via PyArrow")
    
    # Use local storage for LanceDB
    local_db_path = "/opt/lancedb_data/example_db"
    print(f"Connecting to LanceDB with local storage at: {local_db_path}")
    db = lancedb.connect(local_db_path)
    
    # Create larger sample data to meet the minimum requirement for indexing (256 rows)
    num_vectors = 300  # More than the required 256
    vector_dim = 3
    
    # Generate random vectors and IDs
    ids = list(range(1, num_vectors + 1))
    vectors = [list(np.random.rand(vector_dim)) for _ in range(num_vectors)]
    metadata = [f"item{i}" for i in range(1, num_vectors + 1)]
    
    data = {
        "id": ids,
        "vector": vectors,
        "metadata": metadata
    }
    
    df = pd.DataFrame(data)
    
    # Step 1: Create a PyArrow table from the DataFrame
    table = pa.Table.from_pandas(df)
    print(f"Created PyArrow table with {len(df)} rows")
    
    # Step 2: Try to connect to HDFS using PyArrow
    # If HDFS is not available, this will fall back to local filesystem
    try:
        print("Attempting to connect to HDFS via PyArrow...")
        # Create HDFS filesystem
        hdfs = fs.HadoopFileSystem(host="localhost", port=9000)
        
        # Create directory in HDFS if it doesn't exist
        hdfs_path = "/pyarrow_lancedb_data"
        try:
            hdfs.create_dir(hdfs_path)
            print(f"Created directory {hdfs_path} in HDFS")
        except:
            print(f"Directory {hdfs_path} already exists in HDFS")
        
        # Write the PyArrow table to Parquet in HDFS
        parquet_path = f"{hdfs_path}/vector_data.parquet"
        print(f"Writing data to HDFS at {parquet_path}")
        pq.write_table(table, parquet_path, filesystem=hdfs)
        print(f"Successfully wrote data to HDFS at {parquet_path}")
        
        # Read the data back from HDFS
        print(f"Reading data back from HDFS at {parquet_path}")
        hdfs_table = pq.read_table(parquet_path, filesystem=hdfs)
        hdfs_df = hdfs_table.to_pandas()
        print(f"Successfully read {len(hdfs_df)} rows from HDFS")
        
        # Use the data read from HDFS with LanceDB
        print("Creating LanceDB table from HDFS data")
        lancedb_table = db.create_table("hdfs_data_table", data=hdfs_df, mode="overwrite")
        
    except Exception as e:
        print(f"HDFS connection failed: {e}")
        print("Falling back to local filesystem for demonstration")
        # Create LanceDB table directly from the original DataFrame
        lancedb_table = db.create_table("sample_table", data=df, mode="overwrite")
    
    # Add vector index for similarity search
    print("Creating vector index...")
    lancedb_table.create_index(
        metric="cosine",
        vector_column_name="vector"
    )
    print("Vector index created successfully")
    
    # Perform a vector search
    query_vector = [0.5, 0.5, 0.5]  # Sample query vector
    print("Performing vector search...")
    results = lancedb_table.search(query_vector).limit(5).to_pandas()
    
    print("\nSearch Results:")
    print(results)
    
    # List all tables in the database
    print("\nAvailable tables:")
    print(db.table_names())
    
    print("\nExample completed successfully!")
    
    print("""
Integration between LanceDB and Hadoop:
---------------------------------------
1. This example demonstrates using PyArrow as a bridge between Hadoop and LanceDB
2. PyArrow supports HDFS, allowing you to read/write data from/to Hadoop
3. The workflow is:
   - Process data in Hadoop
   - Write results to HDFS in a format like Parquet
   - Use PyArrow to read the data from HDFS
   - Load the data into LanceDB for vector search
4. This approach allows you to leverage both:
   - Hadoop's distributed processing capabilities
   - LanceDB's vector search capabilities
""")

if __name__ == "__main__":
    main()
