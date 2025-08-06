#!/usr/bin/env python3
# Multi-Cloud Aggregation Script
# 
# This script processes an aggregations.json file from an input bucket to aggregate Parquet data to trip summary level.
#
import os
import sys
import logging
from modules.aggregation import AggregateData

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()

def run_container():
    input_bucket = os.environ.get("INPUT_BUCKET")
    output_bucket = input_bucket + "-parquet"
    cloud = os.environ.get("CLOUD")

    logger.info(f"Starting job with input {input_bucket} on {cloud} cloud")

    try:
        # Extract storage client depending on cloud
        if cloud == "Azure":
            from azure.storage.blob import BlobServiceClient
            logging.getLogger('azure').setLevel(logging.WARNING)
            logging.getLogger('azure.core.pipeline').setLevel(logging.ERROR)
            logging.getLogger('azure.storage').setLevel(logging.WARNING)
            connection_string = os.environ.get("StorageConnectionString", "")
            storage_client = BlobServiceClient.from_connection_string(connection_string)
        elif cloud == "Amazon":
            import boto3
            storage_client = boto3.client('s3')
        elif cloud == "Google":
            from google.cloud import storage
            storage_client = storage.Client()
        
        else:
            logger.error(f"Unsupported cloud provider: {cloud}. Supported options: Azure, Amazon, Google")
            return 1
        
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
        logger.error(f"Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(run_container())
