import functions_framework
import os
from google.cloud import storage
from modules.mdf_to_parquet import mdf_to_parquet

# Cloud provider configuration
cloud = "Google"
storage_client = storage.Client()
notification_client = True  # events published via logging functionality

@functions_framework.cloud_event
def process_mdf_file(cloud_event):
    bucket_input = os.environ.get('INPUT_BUCKET')
    bucket_output = bucket_input + "-parquet"
    
    return mdf_to_parquet(cloud, storage_client, notification_client, cloud_event, bucket_input, bucket_output)