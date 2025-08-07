import os
import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from modules.mdf_to_parquet import mdf_to_parquet

# Configure logging to reduce Azure SDK verbosity
logging.getLogger('azure').setLevel(logging.WARNING)
logging.getLogger('azure.core.pipeline').setLevel(logging.ERROR)
logging.getLogger('azure.storage').setLevel(logging.WARNING)

# Cloud provider configuration
storage_connection_string = os.getenv("StorageConnectionString")
bucket_input = os.getenv("INPUT_BUCKET")

cloud = "Azure"
bucket_output = bucket_input + "-parquet"
storage_client = BlobServiceClient.from_connection_string(storage_connection_string)
notification_client = True # events published via logging functionality

app = func.FunctionApp()

@app.function_name(name="ProcessMdfToParquet")
@app.event_grid_trigger(arg_name="event")
def MdfToParquet(event):
    logging.info(f"bucket_input: {bucket_input}")
    if not isinstance(event, list):
        logging.info(f"Processing Event Grid event: {event.event_type}")
    mdf_to_parquet(cloud, storage_client, notification_client, event, bucket_input, bucket_output)