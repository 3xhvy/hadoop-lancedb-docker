from image_search_model import process_cifar10_with_spark
from load_data import load_embeddings_to_lancedb, sample_search
import time


def full_pipeline():
    # Step 1: Prepare data and extract embeddings
    output_parquet = "/hadoop_lancedb_data/cifar10_embeddings.parquet"
    lancedb_path = "/opt/lancedb_data/cifar10_db"
    print("\n=== [Step 1] Processing and saving embeddings... ===")
    t0 = time.time()
    process_cifar10_with_spark(output_parquet, max_train=10000, max_test=2000)
    print(f"Embeddings processed and saved in {time.time() - t0:.2f} seconds\n")

    # Step 2: Load embeddings to LanceDB
    print("=== [Step 2] Loading embeddings into LanceDB... ===")
    t0 = time.time()
    db, tbl = load_embeddings_to_lancedb(output_parquet, lancedb_path)
    print(f"Loaded into LanceDB in {time.time() - t0:.2f} seconds\n")

    # Step 3: Basic LanceDB queries
    print("=== [Step 3] Performing basic LanceDB queries... ===")
    df = tbl.to_pandas()
    print("Sample by ID (id=0):\n", df[df['id'] == 0])
    print("Sample by label (dog):\n", df[df['label_name'] == 'dog'].head())

    # Step 4: Similarity search & evaluation
    print("=== [Step 4] Running similarity search and evaluating... ===")
    t0 = time.time()
    results = sample_search(db, query_id=None)
    duration = time.time() - t0
    correct = results[results['label_name'] == results.iloc[0]['label_name']]
    accuracy = len(correct) / len(results) if len(results) > 0 else 0
    print(f"Search accuracy: {accuracy:.2f} ({len(correct)}/{len(results)})")
    print(f"Search time: {duration:.2f} seconds\n")

if __name__ == "__main__":
    full_pipeline()
