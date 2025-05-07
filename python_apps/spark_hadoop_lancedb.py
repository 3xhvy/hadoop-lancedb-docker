#!/usr/bin/env python3
"""
Example script demonstrating how to load data from Hadoop to LanceDB using PySpark
"""
import os
import pandas as pd
import numpy as np
import lancedb
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, lit, rand
from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, FloatType, StringType

def main():
    print("Loading data from Hadoop to LanceDB using PySpark")

    # Step 1: Initialize Spark with Hadoop configuration
    print("Initializing Spark with Hadoop configuration...")
    spark = SparkSession.builder \
        .appName("Hadoop-LanceDB-Integration") \
        .master("spark://172.18.0.2:7077") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .getOrCreate()

    # Step 2: Generate sample data using Spark
    # In a real scenario, you would read existing data from HDFS
    print("Generating sample data with Spark...")

    # Define schema for our vector data
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("vector", ArrayType(FloatType()), False),
        StructField("metadata", StringType(), False)
    ])

    # Create a sample dataframe with random vectors
    # In a real scenario, you would use: spark.read.parquet("hdfs:///path/to/data")
    num_vectors = 1000
    vector_dim = 16  # Using dimension divisible by 8 to avoid PQ performance warning

    # Create data using Spark's native functions
    spark_df = spark.range(1, num_vectors + 1) \
        .withColumn("id", col("id").cast(IntegerType())) \
        .withColumn("vector", array(*[rand() for _ in range(vector_dim)])) \
        .withColumn("metadata", concat(lit("item"), col("id").cast(StringType())))

    # Show sample data
    print("Sample data generated with Spark:")
    spark_df.show(5)
    print(f"Total rows: {spark_df.count()}")

    # Step 3: Write the data to HDFS in Parquet format
    hdfs_output_path = "/hadoop_lancedb_data/vectors.parquet"
    print(f"Writing data to HDFS at {hdfs_output_path}...")

    # Write to HDFS
    spark_df.write.mode("overwrite").parquet(hdfs_output_path)
    print("Data successfully written to HDFS")

    # Step 4: Read the data back from HDFS
    print(f"Reading data back from HDFS at {hdfs_output_path}...")
    hdfs_df = spark.read.parquet(hdfs_output_path)
    print(f"Successfully read {hdfs_df.count()} rows from HDFS")

    # Step 5: Process the data with Spark (optional)
    # Here you could perform additional transformations using Spark
    print("Processing data with Spark...")

    # Example: Filter vectors with first dimension > 0.5
    filtered_df = hdfs_df.filter(col("vector")[0] > 0.5)
    print(f"Filtered to {filtered_df.count()} rows where first vector dimension > 0.5")

    # Step 6: Convert to pandas DataFrame for LanceDB
    # For large datasets, you might want to process in batches
    print("Converting to pandas DataFrame...")

    # Option 1: Convert directly (for smaller datasets)
    pandas_df = filtered_df.toPandas()

    # Option 2: For larger datasets, you could process in batches
    # batch_size = 10000
    # pandas_dfs = []
    # for batch_df in [filtered_df.limit(batch_size).offset(i)
    #                  for i in range(0, filtered_df.count(), batch_size)]:
    #     pandas_dfs.append(batch_df.toPandas())
    # pandas_df = pd.concat(pandas_dfs)

    print(f"Converted {len(pandas_df)} rows to pandas DataFrame")

    # Step 7: Load the data into LanceDB
    print("Loading data into LanceDB...")
    local_db_path = "/opt/lancedb_data/spark_example_db"
    db = lancedb.connect(local_db_path)

    # Create or overwrite the table
    table = db.create_table("spark_hdfs_vectors", data=pandas_df, mode="overwrite")
    print(f"Created LanceDB table 'spark_hdfs_vectors' with {len(pandas_df)} rows")

    # Step 8: Create a vector index for similarity search
    print("Creating vector index...")
    table.create_index(
        metric="cosine",
        vector_column_name="vector"
    )
    print("Vector index created successfully")

    # Step 9: Perform a vector search
    query_vector = [0.5] * vector_dim  # Sample query vector
    print("Performing vector search...")
    results = table.search(query_vector).limit(5).to_pandas()

    print("\nSearch Results:")
    print(results)

    # Clean up Spark
    spark.stop()

    print("""
Hadoop to LanceDB Integration with PySpark:
------------------------------------------
1. This example demonstrates using PySpark to bridge Hadoop and LanceDB
2. Benefits of this approach:
   - Scalable processing of large datasets with Spark
   - Distributed data processing capabilities of Hadoop
   - Vector search capabilities of LanceDB
3. The workflow is:
   - Read/process data in HDFS using Spark
   - Apply transformations and filtering with Spark
   - Convert the results to a format LanceDB can use
   - Load the data into LanceDB for vector search
4. For very large datasets:
   - Process data in batches
   - Use Spark's partitioning capabilities
   - Consider using PyArrow for more efficient memory management
""")

# Helper function for string concatenation in Spark
def concat(lit_col, col_to_concat):
    from pyspark.sql.functions import concat as spark_concat
    return spark_concat(lit_col, col_to_concat)

if __name__ == "__main__":
    main()
