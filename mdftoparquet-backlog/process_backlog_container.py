#!/usr/bin/env python3
# Multi-Cloud Backlog Processing Script
# 
# This script processes a backlog.json file from cloud storage to convert MDF files to Parquet format.
# The backlog.json should be located in the root of the input container/bucket.
#
# Usage: python process_backlog_container.py --input-bucket <container-name> --cloud <Amazon|Azure>
import os
import sys
import logging
from modules.utils import ProcessBacklog

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()

def run_container():
    input_bucket = os.environ.get("INPUT_BUCKET")
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
            logger.error(f"Unsupported cloud provider: {cloud}. Supported options: Azure, Amazon")
            return 1
        
        pb = ProcessBacklog(cloud, storage_client, input_bucket, logger)
        success = pb.process_backlog_from_cloud()
        return 0 if success else 1
            
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(run_container())
