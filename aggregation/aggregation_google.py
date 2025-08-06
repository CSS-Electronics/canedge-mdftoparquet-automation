#!/usr/bin/env python3
# Google Cloud Storage Aggregation Script
# 
# This script processes an aggregations.json file from a Google Cloud Storage bucket to aggregate Parquet data to trip summary level.
# The aggregations.json should be located in the root of the input bucket.
#
# This is implemented as an HTTP-triggered Cloud Function.
import os
import sys
import logging
import functions_framework
from google.cloud import storage
from modules.utils import AggregateData

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()

def run_aggregation():
    input_bucket = os.environ.get("INPUT_BUCKET")
    output_bucket = input_bucket + "-parquet"
    cloud = "Google"
    try:
        storage_client = storage.Client()
        aggregator = AggregateData(
            cloud=cloud,
            client=storage_client,
            input_bucket=input_bucket,
            output_bucket=output_bucket,
            logger=logger
        )        
        aggregator.process_data_lake()
        return 0 
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return 1

# Google Cloud Function (HTTP trigger)
@functions_framework.http
def run_aggregation(request):
    result = run_aggregation()
    
    if result == 0:
        return {"status": "success", "message": "Aggregation completed successfully"}, 200
    else:
        return {"status": "error", "message": "Aggregation failed"}, 500
   
# Local testing     
if __name__ == "__main__":
    sys.exit(run_aggregation())
