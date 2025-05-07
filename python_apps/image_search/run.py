#!/usr/bin/env python3
"""
CIFAR-10 Image Search Pipeline with Hadoop and LanceDB

This script orchestrates the complete pipeline:
1. Process CIFAR-10 images and extract embeddings
2. Store embeddings in HDFS
3. Load embeddings from HDFS to LanceDB
4. Perform similarity search
"""

import os
import argparse
import time
import numpy as np
import pandas as pd
import lancedb

# Import local modules
from image_search_model import process_cifar10_with_spark
from load_data import load_embeddings_to_lancedb, sample_search

def run_pipeline(query_id=None, force_reprocess=False, max_train=500, max_test=50):
    """
    Run the complete CIFAR-10 image search pipeline
    
    Args:
        query_id: ID of the image to use as query, if None a random one is selected
        force_reprocess: If True, reprocess the CIFAR-10 dataset even if embeddings exist
        max_train: Maximum number of training images to process
        max_test: Maximum number of test images to process
    """
    print("\n" + "=" * 80)
    print("CIFAR-10 Image Search Pipeline with Hadoop and LanceDB")
    print("=" * 80)
    
    # Step 1: Process CIFAR-10 and store embeddings in HDFS
    hdfs_path = "/hadoop_lancedb_data/cifar10_embeddings.parquet"
    lancedb_path = "/opt/lancedb_data/cifar10_db"
    
    # Check if we need to process the dataset
    db = lancedb.connect(lancedb_path)
    table_exists = "cifar10_images" in db.table_names()
    
    if force_reprocess or not table_exists:
        print("\n[Step 1] Processing CIFAR-10 dataset...")
        start_time = time.time()
        hdfs_path = process_cifar10_with_spark(hdfs_path, max_train=max_train, max_test=max_test)
        processing_time = time.time() - start_time
        print(f"Processing completed in {processing_time:.2f} seconds")
        
        # Step 2: Load embeddings from HDFS to LanceDB
        print("\n[Step 2] Loading embeddings from HDFS to LanceDB...")
        start_time = time.time()
        db, _ = load_embeddings_to_lancedb(hdfs_path, lancedb_path)
        loading_time = time.time() - start_time
        print(f"Loading completed in {loading_time:.2f} seconds")
    else:
        print("\n[Step 1 & 2] Skipping processing - CIFAR-10 embeddings already exist in LanceDB")
        print(f"To force reprocessing, use the --force flag")
    
    # Step 3: Perform similarity search
    print("\n[Step 3] Performing similarity search...")
    start_time = time.time()
    results = sample_search(db, query_id)
    search_time = time.time() - start_time
    print(f"Search completed in {search_time:.2f} seconds")
    
    print("\n" + "=" * 80)
    print("Pipeline completed successfully!")
    print("=" * 80)
    
    return results

def explain_architecture():
    """
    Explain the architecture of the CIFAR-10 image search pipeline
    """
    explanation = """
    CIFAR-10 Image Search Architecture with Hadoop and LanceDB
    ======================================================
    
    This application demonstrates how to use Hadoop and LanceDB together for image search:
    
    1. Data Processing with PyTorch and HDFS:
       - CIFAR-10 dataset is loaded and processed using PyTorch
       - ResNet18 model extracts feature vectors (embeddings) from images
       - Embeddings are stored in HDFS as Parquet files using PyArrow
    
    2. Vector Database with LanceDB:
       - Embeddings are loaded from HDFS into LanceDB using PyArrow
       - LanceDB creates a vector index for fast similarity search
       - Queries use cosine similarity to find similar images
    
    3. Integration Points:
       - PyArrow provides the bridge between HDFS and LanceDB
       - HDFS provides distributed storage for the embeddings
       - LanceDB provides efficient vector similarity search
    
    Benefits of this Architecture:
    - Scalable: Can handle large image datasets using Hadoop's distributed storage
    - Fast: LanceDB provides efficient vector similarity search
    - Flexible: Can be extended to other datasets and embedding models
    - End-to-end: Demonstrates the complete pipeline from raw data to search results
    """
    
    print(explanation)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CIFAR-10 Image Search with Hadoop and LanceDB")
    parser.add_argument("--query-id", type=int, help="ID of the image to use as query")
    parser.add_argument("--force", action="store_true", help="Force reprocessing of the dataset")
    parser.add_argument("--explain", action="store_true", help="Explain the architecture")
    parser.add_argument("--max-train", type=int, default=500, help="Maximum number of training images to process")
    parser.add_argument("--max-test", type=int, default=50, help="Maximum number of test images to process")
    
    args = parser.parse_args()
    
    if args.explain:
        explain_architecture()
    
    run_pipeline(query_id=args.query_id, force_reprocess=args.force, 
                max_train=args.max_train, max_test=args.max_test)
