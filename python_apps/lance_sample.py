# lance_sample.py
# Sample script to demonstrate local usage of LanceDB
# and how to connect to Hadoop HDFS using PyArrow

import lancedb
import numpy as np
import os
import pyarrow as pa
import shutil

# --- CONFIGURATION ---
USE_HDFS = True  # Set to True if you want to use HDFS
hdfs_uri = "hdfs://localhost:9000/user/hadoop/lancedb_data"
local_uri = "./lancedb_data"
table_name = "my_table"
vector_length = 4


def check_env():
    print("[INFO] Validating Hadoop environment...")

    hadoop_home = os.getenv("HADOOP_HOME")
    classpath = os.getenv("CLASSPATH")
    if not hadoop_home:
        print("[WARN] $HADOOP_HOME is not set. PyArrow may fail to find libhdfs.")
    else:
        print(f"[OK] HADOOP_HOME = {hadoop_home}")

    if not classpath:
        print("[WARN] $CLASSPATH is not set. Attempting to auto-populate it.")
        try:
            from subprocess import check_output
            cp = check_output(["hadoop", "classpath", "--glob"]).decode("utf-8").strip()
            os.environ["CLASSPATH"] = cp
            print("[OK] CLASSPATH set dynamically.")
        except Exception as e:
            print("[ERROR] Could not auto-generate CLASSPATH:", e)

    # Check for libhdfs.so
    libhdfs_found = False
    if hadoop_home:
        for root, dirs, files in os.walk(hadoop_home):
            if "libhdfs.so" in files:
                print(f"[OK] Found libhdfs.so at {os.path.join(root, 'libhdfs.so')}")
                libhdfs_found = True
                break
    if not libhdfs_found:
        print("[WARN] libhdfs.so not found. Set LD_LIBRARY_PATH if needed.")


# Optional: run Hadoop/PyArrow env check
if USE_HDFS:
    check_env()
    try:
        fs = pa.hdfs.connect("localhost", 9000, user="hadoop")
        print("[HDFS] Connection successful. Listing /user/hadoop:")
        print(fs.ls("/user/hadoop"))
    except Exception as e:
        print("[ERROR] Could not connect to HDFS via PyArrow:", e)
        print("Check $HADOOP_HOME, $CLASSPATH, libhdfs.so, and Hadoop status.")
        exit(1)

# --- Connect to LanceDB ---
uri = hdfs_uri if USE_HDFS else local_uri
db = lancedb.connect(uri)

# Drop table if exists
if table_name in db.table_names():
    db.drop_table(table_name)

# Sample data
sample_data = [
    {"vector": np.random.rand(4).tolist(), "id": 1, "name": "Alice"},
    {"vector": np.random.rand(4).tolist(), "id": 2, "name": "Bob"},
    {"vector": np.random.rand(4).tolist(), "id": 3, "name": "Charlie"},
]

# Create new table
table = db.create_table(
    table_name,
    data=sample_data,
    schema=None,
    mode="overwrite",
    vector_column_name="vector",
    vector_column_dim=vector_length,
)

# Insert new row
new_data = {"vector": np.random.rand(4).tolist(), "id": 4, "name": "Diana"}
table.add([new_data])

# Vector search
query_vector = np.random.rand(4).tolist()
print(f"\nQuery vector: {query_vector}")
results = table.search(query_vector).limit(2).to_df()
print("Top 2 most similar rows:")
print(results)

# Show all rows
print("\nAll data in table:")
print(table.to_df())

# Info message
if USE_HDFS:
    print(f"\n[INFO] Running with HDFS. LanceDB URI: {hdfs_uri}")
else:
    print(f"\n[INFO] Running locally. LanceDB URI: {local_uri}")
