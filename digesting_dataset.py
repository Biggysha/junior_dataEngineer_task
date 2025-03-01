import argparse
import logging
from time import time

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from datasets import load_dataset

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_from_huggingface(dataset_name, split='train', subset=None):
    """Load dataset from HuggingFace datasets"""
    try:
        logging.info(f"Loading dataset {dataset_name} from HuggingFace")
        if subset:
            dataset = load_dataset(dataset_name, subset, split=split)
        else:
            dataset = load_dataset(dataset_name, split=split)
        df = dataset.to_pandas()
        return df
    except Exception as e:
        logging.error(f"Failed to load dataset from HuggingFace: {str(e)}")
        raise

def create_db_engine(params):
    """Create database engine with error handling"""
    try:
        engine = create_engine(
            f'postgresql://{params.user}:{params.password}@{params.host}:{params.port}/{params.db}'
        )
        # Test the connection
        with engine.connect() as conn:
            pass
        return engine
    except SQLAlchemyError as e:
        raise ConnectionError(f"Failed to connect to database: {str(e)}")

def main(params):
    try:
        # Load data from HuggingFace
        df = load_from_huggingface(
            params.dataset_name,
            params.split,
            params.subset
        )
        
        # Convert to iterator for chunked processing
        df_iter = [df[i:i + 100000] for i in range(0, len(df), 100000)]
        df_iter = iter(df_iter)
        
        logging.info("Data loading completed successfully")

        # Create database engine
        engine = create_db_engine(params)
        logging.info("Database connection established")

        # Process chunks
        chunk_number = 0
        total_rows = 0
        
        while True:
            try:
                t_start = time()
                df = next(df_iter)
                
                # Rename columns according to mapping if needed
                if hasattr(params, 'columns_mapping'):
                    df = df.rename(columns=params.columns_mapping)
                
                df.to_sql(
                    name=params.table_name,
                    con=engine,
                    if_exists='append' if chunk_number > 0 else 'replace',
                    index=False
                )
                
                chunk_number += 1
                total_rows += len(df)
                t_end = time()
                
                logging.info(f'Chunk {chunk_number} inserted ({len(df)} rows), took {(t_end - t_start):.3f} seconds')
                
            except StopIteration:
                logging.info(f'Data ingestion completed successfully. Total rows processed: {total_rows}')
                break
            except Exception as e:
                logging.error(f"Error processing chunk {chunk_number}: {str(e)}")
                raise
                
    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest data from HuggingFace to Postgres')
    parser.add_argument('--dataset_name', required=True, help='Name of the dataset in HuggingFace')
    parser.add_argument('--split', default='train', help='Dataset split to use')
    parser.add_argument('--subset', help='Dataset subset name if applicable')
    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    
    args = parser.parse_args()
    
    try:
        main(args)
    except Exception as e:
        logging.error(str(e))
        exit(1)


#python pipeline.py --dataset_name=openai/gsm8k --split=train --subset=main --user=root --password=root --host=localhost --port=5432 --db=gsm8k --table_name=gsm8k_train
