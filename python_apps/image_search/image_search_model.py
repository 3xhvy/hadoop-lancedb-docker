#!/usr/bin/env python3
"""
Image search model for CIFAR-10 dataset using PySpark and PyTorch
"""

import os
import io
import numpy as np
import torch
import torchvision.models as models
import torchvision.transforms as transforms
import torchvision.datasets as datasets
from PIL import Image
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, FloatType
import pandas as pd
import sys
sys.path.append('/home/hadoop/python_apps/image_search')

# CIFAR-10 class names for reference
CIFAR10_CLASSES = [
    'airplane', 'automobile', 'bird', 'cat', 'deer',
    'dog', 'frog', 'horse', 'ship', 'truck'
]

# Set environment variables for PySpark to use Python 3.10
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3.10'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3.10'

def extract_features(image_bytes):
    """
    Extract feature vector from image bytes using ResNet18

    This function is defined at the module level to avoid serialization issues with PySpark.

    Args:
        image_bytes: Image as bytes

    Returns:
        Feature vector as list
    """
    # Import libraries inside the function to ensure they're available in Spark workers
    import torch
    import torchvision.transforms as transforms
    from torchvision import models
    from PIL import Image
    import io
    import numpy as np

    try:
        # Load pre-trained ResNet model
        if 'model' not in extract_features.__dict__:
            print("Loading ResNet18 model...")
            extract_features.model = models.resnet18(weights='DEFAULT')
            extract_features.model.eval()

        # Define image transformation pipeline
        transform = transforms.Compose([
            transforms.Resize(224),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
        ])

        # Convert bytes to PIL Image
        image = Image.open(io.BytesIO(image_bytes)).convert("RGB")

        # Apply transformations and create batch dimension
        tensor = transform(image).unsqueeze(0)

        # Extract features (without gradients for efficiency)
        with torch.no_grad():
            # Get the features before the final classification layer
            features = extract_features.model(tensor)

        # Convert to list for Spark UDF compatibility
        return features.squeeze().numpy().tolist()
    except Exception as e:
        print(f"Error processing image: {e}")
        # Return zero vector in case of error
        return [0.0] * 1000  # ResNet18 feature size

def process_cifar10_with_spark(output_path="/hadoop_lancedb_data/cifar10_embeddings.parquet",
                              max_train=1000, max_test=100):
    """
    Process CIFAR-10 dataset using PySpark and save embeddings.

    Args:
        output_path: Path to save the embeddings (will attempt HDFS first, then local).
        max_train: Maximum number of training images to process.
        max_test: Maximum number of test images to process.

    Returns:
        Path to the saved embeddings.
    """
    spark = None
    output_df = None
    try:
        # Initialize Spark with Hadoop configuration
        import socket
        import os

        # Create package zip for distribution to workers
        script_dir = os.path.dirname(os.path.abspath(__file__))
        package_zip = os.path.join(script_dir, "image_search_package.zip")
        
        # Always recreate the package zip to ensure latest code
        print("Creating package zip for Spark workers...")
        from create_package import create_package_zip
        create_package_zip(script_dir, package_zip)

        hostname = socket.getfqdn()
        print(f"Initializing Spark with Hadoop configuration... Master URL: spark://{hostname}:7077")
        print(f"Using Python package zip: {package_zip}")

        spark = SparkSession.builder \
            .appName("CIFAR10-Embedding") \
            .master(f"spark://{hostname}:7077") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.pyspark.python", "/usr/bin/python3.10") \
            .config("spark.submit.pyFiles", package_zip) \
            .getOrCreate()

        # Set log level to reduce verbosity
        spark.sparkContext.setLogLevel("WARN")

        print("Loading CIFAR-10 dataset...")
        # Download CIFAR-10 dataset (this will be done on the driver)
        cifar10_train = datasets.CIFAR10(root='./data', train=True, download=True)
        cifar10_test = datasets.CIFAR10(root='./data', train=False, download=True)

        # Prepare data for Spark processing
        print("Preparing data for Spark processing...")
        train_data = []
        for i, (image, label) in enumerate(cifar10_train):
            if i >= max_train:
                break

            # Convert PIL Image to bytes
            img_byte_arr = io.BytesIO()
            image.save(img_byte_arr, format='PNG')
            img_bytes = img_byte_arr.getvalue()

            train_data.append((i, img_bytes, int(label), CIFAR10_CLASSES[label], 'train'))

            if (i + 1) % 100 == 0:
                print(f"Prepared {i + 1} training images")

        test_data = []
        for i, (image, label) in enumerate(cifar10_test):
            if i >= max_test:
                break

            # Convert PIL Image to bytes
            img_byte_arr = io.BytesIO()
            image.save(img_byte_arr, format='PNG')
            img_bytes = img_byte_arr.getvalue()

            test_data.append((i + 10000, img_bytes, int(label), CIFAR10_CLASSES[label], 'test'))

            if (i + 1) % 20 == 0:
                print(f"Prepared {i + 1} test images")

        # Combine train and test data
        all_data = train_data + test_data

        # Create DataFrame from the data
        print("Creating Spark DataFrame...")
        df = spark.createDataFrame(
            all_data,
            ["id", "image_bytes", "label_id", "label_name", "split"]
        )

        # Create and register UDF for feature extraction
        print("Creating feature extraction UDF...")
        extract_features_udf = udf(extract_features, ArrayType(FloatType()))

        # Extract features using the UDF
        print("Extracting features from images...")
        df_with_features = df.withColumn("embedding", extract_features_udf(col("image_bytes")))

        # Select relevant columns for output
        output_df = df_with_features.select(
            "id", "label_id", "label_name", "split", "embedding"
        )

        # Show sample of the data
        print("Sample of processed data:")
        output_df.limit(5).select("id", "label_id", "label_name", "split").show()

        # Save DataFrame to output_path (assume HDFS path is valid in this environment)
        print(f"Saving embeddings to: {output_path}")
        output_df.write.mode("overwrite").parquet(output_path)
        print(f"Successfully saved embeddings to {output_path}")
        
        # Also save a local copy as backup
        local_path = "/tmp/cifar10_embeddings.parquet"
        print(f"Saving backup copy to {local_path}")
        output_df.write.mode("overwrite").parquet(local_path)
        print(f"Successfully saved backup to {local_path}")
        
        return output_path

    except Exception as e:
        print(f"Error occurred during processing: {e}")
        if output_df is not None:
            local_path = "/tmp/cifar10_embeddings_fallback.parquet"
            try:
                output_df.write.mode("overwrite").parquet(local_path)
                print(f"Saved embeddings locally due to error: {local_path}")
                return local_path
            except Exception as save_error:
                print(f"Failed to save fallback file: {save_error}")
        else:
            print("DataFrame was not created before error occurred.")
            return None
    finally:
        try:
            # Stop Spark session gracefully
            if spark is not None:
                spark.stop()
        except Exception as e:
            print(f"Warning: Error stopping Spark session: {str(e)}")
        print("Spark session stopped.")

if __name__ == "__main__":
    process_cifar10_with_spark()
