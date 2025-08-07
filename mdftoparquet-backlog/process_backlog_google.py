#!/usr/bin/env python3
# Google Cloud Storage Backlog Processing Script
# 
# This script processes a backlog.json file from a Google Cloud Storage bucket to convert MDF files to Parquet format.
# The backlog.json should be located in the root of the input bucket.
#
# This is implemented as an HTTP-triggered Cloud Function.
import os
import sys
import logging
import functions_framework
from google.cloud import storage
from modules.utils import ProcessBacklog

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def run_backlog_processing():
    input_bucket = os.environ.get("INPUT_BUCKET")
    cloud = "Google"
    try:
        storage_client = storage.Client()
        pb = ProcessBacklog(cloud, storage_client, input_bucket, logger)
        pb.process_backlog_from_cloud()
        return 0 
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return 1

# Google Cloud Function (HTTP trigger)
@functions_framework.http
def process_mdf_file(request):
    result = run_backlog_processing()
    
    if result == 0:
        return {"status": "success", "message": "Backlog processing completed successfully"}, 200
    else:
        return {"status": "error", "message": "Backlog processing failed"}, 500
   
# Local testing     
if __name__ == "__main__":
    sys.exit(run_backlog_processing())
