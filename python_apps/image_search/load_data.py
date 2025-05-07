#!/usr/bin/env python3
"""
Load CIFAR-10 embeddings from HDFS into LanceDB
"""

import os
import time
import lancedb
import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq
import pandas as pd
import numpy as np
from pyarrow import schema, field
from pyarrow import list_, float32

def load_embeddings_to_lancedb(hdfs_path="/hadoop_lancedb_data/cifar10_embeddings.parquet",
                              lancedb_path="/opt/lancedb_data/cifar10_db"):
    """
    Load embeddings from HDFS into LanceDB

    Args:
        hdfs_path: Path to the embeddings in HDFS
        lancedb_path: Path to store the LanceDB database
    """
    print(f"Loading embeddings from HDFS: {hdfs_path}")

    # Set required environment variables for HDFS
    if 'HADOOP_HOME' not in os.environ:
        os.environ['HADOOP_HOME'] = '/opt/hadoop'
    if 'CLASSPATH' not in os.environ:
        os.environ['CLASSPATH'] = f"{os.environ['HADOOP_HOME']}/etc/hadoop:" \
                                 f"{os.environ['HADOOP_HOME']}/share/hadoop/common/lib/*:" \
                                 f"{os.environ['HADOOP_HOME']}/share/hadoop/hdfs:" \
                                 f"{os.environ['HADOOP_HOME']}/share/hadoop/hdfs/lib/*:" \
                                 f"{os.environ['HADOOP_HOME']}/share/hadoop/hdfs/*"

    try:
        # Connect to HDFS using PyArrow with retries
        max_retries = 3
        retry_delay = 2
        last_exception = None

        for attempt in range(max_retries):
            try:
                hdfs = fs.HadoopFileSystem(
                    host="localhost",
                    port=9000,
                    user="root"  # Explicitly set user
                )
                # Test connection
                parent_dir = os.path.dirname(hdfs_path)
                hdfs.get_file_info(fs.FileSelector(parent_dir))
                break
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:
                    print(
                        f"HDFS connection attempt {attempt + 1} failed. Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    raise Exception(
                        f"Failed to connect to HDFS after {max_retries} attempts") from e

        # Check if the file exists
        file_info = hdfs.get_file_info(fs.FileSelector(hdfs_path))
        if not file_info or len(file_info) == 0:
            raise FileNotFoundError(f"Directory not found: {hdfs_path}")

        # Read the Parquet file from HDFS
        print("Reading Parquet file from HDFS...")
        table = pq.read_table(hdfs_path, filesystem=hdfs)

        # Convert to pandas DataFrame for easier manipulation
        df = table.to_pandas()
        print(f"Loaded {len(df)} embeddings from HDFS")

        # Prepare embeddings for LanceDB
        print("Preparing embeddings for LanceDB...")

        # Get the dimension of embeddings
        sample_embedding = df['embedding'].iloc[0]
        if isinstance(sample_embedding, list):
            vector_dim = len(sample_embedding)
            print(f"Embedding dimension: {vector_dim}")
        else:
            vector_dim = len(sample_embedding.tolist())
            print(f"Embedding dimension: {vector_dim}")

        # Convert embeddings to proper format for LanceDB
        print("Converting embeddings to LanceDB-compatible format...")

        # First convert to numpy arrays if they aren't already
        df['embedding'] = df['embedding'].apply(lambda x: np.array(x, dtype=np.float32))

        # Then convert to PyArrow FixedSizeListArray
        embeddings_array = pa.FixedSizeListArray.from_arrays(
            pa.concat_arrays([pa.array(embedding) for embedding in df['embedding']]),
            list_size=vector_dim
        )

        # Create a new Arrow table with the proper schema
        schema = pa.schema([
            pa.field('id', pa.int32()),
            pa.field('label', pa.int32()),
            pa.field('label_name', pa.string()),
            pa.field('embedding', pa.list_(pa.float32(), vector_dim))
        ])

        # Create the Arrow table
        table = pa.Table.from_pandas(df.drop(columns=['embedding']), preserve_index=False)
        table = table.append_column('embedding', embeddings_array)

        # Create LanceDB database
        print(f"Connecting to LanceDB at {lancedb_path}")
        db = lancedb.connect(lancedb_path)

        # Create or overwrite the table
        print("Creating LanceDB table 'cifar10_images'")
        table_name = "cifar10_images"

        # Create or overwrite the table with schema definition
        if table_name in db.table_names():
            print(f"Table '{table_name}' already exists, deleting...")
            db.drop_table(table_name)

        # Create a new table with proper schema
        print(f"Creating new table '{table_name}' with vector schema...")
        tbl = db.create_table(table_name, data=table)

        # Create vector index for similarity search
        print("Creating vector index...")
        try:
            # Try with a simpler index type first
            tbl.create_index(
                vector_column_name="embedding",
                metric="cosine",
                index_type="IVF",
                num_partitions=8
            )
            print("Created IVF index successfully")
        except Exception as e:
            print(f"Warning: Could not create IVF index: {e}")
            try:
                # Fall back to HNSW index which works better with list data
                tbl.create_index(
                    vector_column_name="embedding",
                    metric="cosine",
                    index_type="HNSW"
                )
                print("Created HNSW index successfully")
            except Exception as e2:
                print(f"Warning: Could not create HNSW index: {e2}")
                print("Using brute force search (no index)")

        print(f"Successfully loaded data into LanceDB table '{table_name}'")
        print(f"Available tables: {db.table_names()}")

        return db, tbl

    except Exception as e:
        print(f"Error loading embeddings: {e}")
        raise

def sample_search(db, query_id=None):
    """
    Perform a sample similarity search on the CIFAR-10 embeddings.

    Args:
        db: LanceDB database connection
        query_id: ID of the image to use as query, if None a random one is selected

    Returns:
        DataFrame with search results
    """
    try:
        # Import numpy at the top of the function
        import numpy as np
        import random

        # Open the table
        tbl = db.open_table("cifar10_images")

        # Get all data for selecting a query
        df = tbl.to_pandas()

        if query_id is None:
            # Select a random image as query
            query_idx = np.random.randint(0, len(df))
            query_row = df.iloc[query_idx]
        else:
            # Use the specified image as query
            query_row = df[df['id'] == query_id].iloc[0]

        query_vector = query_row['embedding']
        query_label = query_row['label_name']

        print(f"\nPerforming search with query image (ID: {query_row['id']}, Class: {query_label})")

        # Convert query vector to numpy array if it's a list
        if isinstance(query_vector, list):
            query_vector = np.array(query_vector, dtype=np.float32)

        # Search for similar images - explicitly specify vector column name
        results = tbl.search(query_vector, vector_column_name="embedding").limit(10).to_pandas()

        print("\nTop 10 similar images:")
        print(results[['id', 'label_name', '_distance']])

        # Calculate accuracy of the search results
        correct_matches = results[results['label_name'] == query_label]
        accuracy = len(correct_matches) / len(results)

        print(f"\nSearch accuracy: {accuracy:.2f} ({len(correct_matches)} correct matches out of {len(results)})")

        return results

    except Exception as e:
        print(f"Error performing search: {e}")
        raise

if __name__ == "__main__":
    # Load embeddings to LanceDB
    db, _ = load_embeddings_to_lancedb()

    # Perform sample search
    sample_search(db)
