#!/usr/bin/env python3
# Google Cloud Storage Aggregation Script
# 
# This script processes an aggregations.json file from an input bucket to aggregate Parquet data to trip summary level.
#
# This is implemented as an HTTP-triggered Cloud Function.
import os
import sys
import logging
import functions_framework
from google.cloud import storage
from modules.aggregation import AggregateData

def run_aggregation(logger):
    input_bucket = os.environ.get("INPUT_BUCKET")
    output_bucket = input_bucket + "-parquet"
    cloud = "Google"
    logger.info(f"Starting aggregation for {output_bucket}")

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
def http_aggregation(request):
    cloud_logger = logging.getLogger()
    cloud_logger.setLevel(logging.INFO)
    result = run_aggregation(cloud_logger)
    
    if result == 0:
        return {"status": "success", "message": "Aggregation completed successfully"}, 200
    else:
        return {"status": "error", "message": "Aggregation failed"}, 500
   
# Local testing     
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    local_logger = logging.getLogger()
    sys.exit(run_aggregation(local_logger))
