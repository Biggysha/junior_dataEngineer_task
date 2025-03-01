import logging
from typing import Dict, Any

from digesting_dataset import load_from_huggingface, create_db_engine
from text_preprocessor import TextPreprocessor
from gcp_storage import GCPStorageHandler
from sqlalchemy import text
import pandas as pd
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_table(engine):
    """Create the database table if it doesn't exist"""
    create_table_query = text("""
    CREATE TABLE IF NOT EXISTS processed_dataset (
        id SERIAL PRIMARY KEY,
        original_text TEXT NOT NULL,
        processed_text TEXT NOT NULL,
        token_count INTEGER,
        sentence_count INTEGER,
        unique_tokens INTEGER,
        avg_sentence_length FLOAT,
        max_sentence_length INTEGER,
        min_sentence_length INTEGER,
        chunk_number INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_token_count ON processed_dataset(token_count);
    CREATE INDEX IF NOT EXISTS idx_sentence_count ON processed_dataset(sentence_count);
    """)
    
    with engine.connect() as conn:
        conn.execute(create_table_query)
        conn.commit()
    logging.info("Database table created successfully")

def process_and_load_data(dataset: pd.DataFrame, preprocessor: TextPreprocessor, engine: Any, batch_size: int = 1000):
    """Process the dataset and load it into the database"""
    total_processed = 0
    batch = []
    
    for idx, row in dataset.iterrows():
        # Extract text from various possible column names
        text = row.get('text') or row.get('question') or row.get('content')
        if not text:
            continue
            
        # Process text
        result = preprocessor.process_text(text)
        
        # Skip invalid texts
        if not result['chunks']:
            continue
            
        # Add each chunk to batch
        for chunk_num, chunk in enumerate(result['chunks']):
            batch_item = {
                'original_text': text,
                'processed_text': chunk,
                'chunk_number': chunk_num,
                'token_count': result['stats'].get('token_count', 0),
                'sentence_count': result['stats'].get('sentence_count', 0),
                'unique_tokens': result['stats'].get('unique_tokens', 0),
                'avg_sentence_length': result['stats'].get('avg_sentence_length', 0.0),
                'max_sentence_length': result['stats'].get('max_sentence_length', 0),
                'min_sentence_length': result['stats'].get('min_sentence_length', 0)
            }
            batch.append(batch_item)
            
        # Process batch when it reaches batch_size
        if len(batch) >= batch_size:
            df = pd.DataFrame(batch)
            df.to_sql('processed_dataset', engine, if_exists='append', index=False)
            total_processed += len(batch)
            logging.info(f"Processed {total_processed} texts")
            batch = []
    
    # Process remaining items
    if batch:
        df = pd.DataFrame(batch)
        df.to_sql('processed_dataset', engine, if_exists='append', index=False)
        total_processed += len(batch)
    
    return total_processed

def main(params: Dict):
    try:
        # Step 1: Load dataset from HuggingFace
        logging.info(f"Loading dataset {params['dataset_name']} from HuggingFace")
        dataset = load_from_huggingface(
            params['dataset_name'],
            params['split'],
            params.get('subset')
        )
        logging.info(f"Successfully loaded {len(dataset)} rows from HuggingFace")
        
        # Step 2: Initialize preprocessor and GCP handler
        preprocessor = TextPreprocessor()
        gcp_handler = GCPStorageHandler("burmese-ai6666-54ed5333f7c9.json")
        
        # Step 3: Upload raw data to GCP
        logging.info("Uploading raw data to GCP bucket")
        gcp_handler.upload_dataframe_to_bucket(
            "my-raw-data-bucket",
            dataset,
            f"raw/{params['dataset_name']}_{params['split']}.csv"
        )
        
        # Step 4: Create database connection
        db_params = argparse.Namespace(
            user=params['user'],
            password=params['password'],
            host=params['host'],
            port=params['port'],
            db=params['db']
        )
        engine = create_db_engine(db_params)
        
        # Step 5: Create table
        create_table(engine)
        
        # Step 6: Process and load data
        total_processed = process_and_load_data(
            dataset,
            preprocessor,
            engine,
            params.get('batch_size', 1000)
        )
        
        # Step 7: Upload processed data to GCP
        logging.info("Uploading processed data to GCP bucket")
        processed_df = pd.read_sql('SELECT * FROM processed_dataset', engine)
        gcp_handler.upload_dataframe_to_bucket(
            "my-process-data-bucket",
            processed_df,
            f"processed/{params['dataset_name']}_{params['split']}_processed.csv"
        )
        
        logging.info(f"Pipeline completed successfully. Total processed texts: {total_processed}")
        
    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Complete data processing pipeline')
    parser.add_argument('--dataset_name', required=True, help='Name of the dataset in HuggingFace')
    parser.add_argument('--split', default='train', help='Dataset split to use')
    parser.add_argument('--subset', help='Dataset subset name if applicable')
    parser.add_argument('--user', default='root', help='user name for postgres')
    parser.add_argument('--password', default='root', help='password for postgres')
    parser.add_argument('--host', default='localhost', help='host for postgres')
    parser.add_argument('--port', default='5432', help='port for postgres')
    parser.add_argument('--db', default='gsm8k', help='database name for postgres')
    parser.add_argument('--batch_size', type=int, default=1000, help='Batch size for processing')
    
    args = parser.parse_args()
    
    try:
        main(vars(args))
    except Exception as e:
        logging.error(str(e))
        exit(1)

# cmd
# python main.py --dataset_name="gsm8k" --split="train" --subset="main" --batch_size=1000