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
import argparse
from modules.utils import ProcessBacklog

# Configure the root logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()

def run_container():
    """Main entry point for running in a container environment using environment variables"""
    # Get environment variables
    input_bucket = os.environ.get("INPUT_BUCKET")
    cloud_provider = os.environ.get("CLOUD")
    
    logger.info(f"Starting job with input {input_bucket} on {cloud_provider} cloud")

    try:
        # Initialize cloud storage client based on the provider
        if cloud_provider.lower() == "azure":
            from azure.storage.blob import BlobServiceClient
            logging.getLogger('azure').setLevel(logging.WARNING)
            logging.getLogger('azure.core.pipeline').setLevel(logging.ERROR)
            logging.getLogger('azure.storage').setLevel(logging.WARNING)
            connection_string = os.environ.get("StorageConnectionString", "")
            storage_client = BlobServiceClient.from_connection_string(connection_string)
        
        elif cloud_provider.lower() == "amazon":
            import boto3
            storage_client = boto3.client('s3')
        elif cloud_provider.lower() == "google":
            from google.cloud import storage
            storage_client = storage.Client()
        
        else:
            logger.error(f"Unsupported cloud provider: {cloud_provider}. Supported options: Azure, Amazon")
            return 1
        
        pb = ProcessBacklog(cloud_provider, storage_client, input_bucket, logger)
        success = pb.process_backlog_from_cloud()
        
        if success:
            logger.info("Backlog processing completed successfully")
            return 0
        else:
            logger.error("Backlog processing failed")
            return 1
            
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1

# For local testing via command line - passes args to environment variables
def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process a backlog.json file from cloud storage')
    parser.add_argument('-i', '--input-bucket', required=True, help='Input container/bucket name containing backlog.json')
    parser.add_argument('-c', '--cloud', required=True, choices=['Azure', 'Amazon'], help='Cloud provider to use')
    parser.add_argument('-d', '--decoder', default='mdf2parquet_decode', help='Path to the MF4 decoder executable')
    args = parser.parse_args()
    
    # Set environment variables
    os.environ["INPUT_BUCKET"] = args.input_bucket
    os.environ["CLOUD"] = args.cloud
    os.environ["MF4_DECODER"] = args.decoder
    
    # Call the container entry point
    return run_container()

if __name__ == "__main__":
    logger.info("Starting Multi-Cloud MDF to Parquet backlog processing script")
    # Check if arguments were passed
    if len(sys.argv) > 1:
        logger.info(f"Command line arguments detected: {sys.argv[1:]}")
        sys.exit(main())
    else:
        sys.exit(run_container())
