import pandas as pd
from sqlalchemy import create_engine, text
from gcp_storage import GCPStorageHandler

# 🔹 PostgreSQL Connection Config
DB_HOST = "localhost"  # Change if using a remote database
DB_NAME = "gsm8k"
DB_USER = "root"
DB_PASSWORD = "root"
DB_PORT = "5432"  # Default PostgreSQL port

# 🔹 Initialize GCP Storage Handler
gcp_handler = GCPStorageHandler("burmese-ai6666-54ed5333f7c9.json")
RAW_BUCKET = "my-raw-data-bucket"
PROCESSED_BUCKET = "my-process-data-bucket"

# 🔹 Create Database Connection
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# 🔹 Define Table Schema (Ensure it Exists)
create_table_query = text("""
CREATE TABLE IF NOT EXISTS gsm8k_data (
    id SERIAL PRIMARY KEY,
    original_text TEXT NOT NULL,
    processed_text TEXT NOT NULL,
    token_count INT,
    sentence_count INT,
    avg_sentence_length FLOAT,
    unique_tokens INT,
    created_at TIMESTAMP DEFAULT NOW()
);
""")

# Execute table creation
with engine.connect() as conn:
    conn.execute(create_table_query)
    conn.commit()

# 🔹 Load CSV Files into DataFrame
train_df = pd.read_csv("processed_data/gsm8k_train.csv")
test_df = pd.read_csv("processed_data/gsm8k_test.csv")

# 🔹 Upload raw data to GCP
gcp_handler.upload_dataframe_to_bucket(RAW_BUCKET, train_df, "raw/gsm8k_train.csv")
gcp_handler.upload_dataframe_to_bucket(RAW_BUCKET, test_df, "raw/gsm8k_test.csv")

# 🔹 Select Relevant Columns
columns_mapping = {
    "Original": "original_text",
    "Processed": "processed_text",
    "token_count": "token_count",
    "sentence_count": "sentence_count",
    "avg_sentence_length": "avg_sentence_length",
    "unique_tokens": "unique_tokens",
}

train_df = train_df.rename(columns=columns_mapping)
test_df = test_df.rename(columns=columns_mapping)

# 🔹 Append Data to PostgreSQL
train_df.to_sql("gsm8k_data", engine, if_exists="append", index=False)
test_df.to_sql("gsm8k_data", engine, if_exists="append", index=False)

# 🔹 Upload processed data to GCP
gcp_handler.upload_dataframe_to_bucket(PROCESSED_BUCKET, train_df, "processed/gsm8k_train.csv")
gcp_handler.upload_dataframe_to_bucket(PROCESSED_BUCKET, test_df, "processed/gsm8k_test.csv")

print("✅ Data successfully loaded into PostgreSQL and GCP Storage!")