from google.cloud import storage
import pandas as pd
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class GCPStorageHandler:
    def __init__(self, credentials_path):
        """Initialize GCP storage client"""
        self.storage_client = storage.Client.from_service_account_json(credentials_path)
        
    def upload_file_to_bucket(self, bucket_name: str, source_file_path: str, destination_blob_name: str) -> None:
        """Upload a file to GCP bucket"""
        try:
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            
            blob.upload_from_filename(source_file_path)
            logging.info(
                f"File {source_file_path} uploaded to {destination_blob_name} in bucket {bucket_name}"
            )
        except Exception as e:
            logging.error(f"Failed to upload file to GCP: {str(e)}")
            raise
    
    def upload_dataframe_to_bucket(self, bucket_name: str, df: pd.DataFrame, destination_blob_name: str) -> None:
        """Upload a pandas DataFrame to GCP bucket as CSV"""
        try:
            # Save DataFrame to temporary CSV file
            temp_file = 'temp_data.csv'
            df.to_csv(temp_file, index=False)
            
            # Upload to GCP
            self.upload_file_to_bucket(bucket_name, temp_file, destination_blob_name)
            
            # Clean up temporary file
            os.remove(temp_file)
        except Exception as e:
            logging.error(f"Failed to upload DataFrame to GCP: {str(e)}")
            if os.path.exists('temp_data.csv'):
                os.remove('temp_data.csv')
            raise