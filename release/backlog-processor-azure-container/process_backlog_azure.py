#!/usr/bin/env python3
# Azure Blob Storage Backlog Processing Script
# 
# This script processes a backlog.json file from an Azure blob container to convert MDF files to Parquet format.
# The backlog.json should be located in the root of the input container.
#
# Usage: python process_backlog_azure.py --input-bucket <container-name>
import os
import sys
import logging
import argparse
from azure.storage.blob import BlobServiceClient
from modules.utils import ProcessBacklog


# Configure the root logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger('azure').setLevel(logging.WARNING)
logging.getLogger('azure.core.pipeline').setLevel(logging.ERROR)
logging.getLogger('azure.storage').setLevel(logging.WARNING)
logger = logging.getLogger()

def run_container():
    """Main entry point for running in a container environment using environment variables"""
    # Get environment variables
    input_bucket = os.environ.get("INPUT_BUCKET")
    
    if not input_bucket:
        logger.error("Missing required environment variable: INPUT_BUCKET")
        return 1
    
    # Initialize Azure Blob Storage client
    try:
        connection_string = os.environ.get("StorageConnectionString", "")
        if not connection_string:
            logger.error("StorageConnectionString environment variable not set")
            return 1
            
        storage_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Process the backlog
        logger.info(f"Starting backlog processing for container: {input_bucket}")
        pb = ProcessBacklog("Azure", storage_client, input_bucket, logger)
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
    parser = argparse.ArgumentParser(description='Process a backlog.json file from an Azure blob container')
    parser.add_argument('-i', '--input-bucket', required=True, help='Input container name containing backlog.json')
    parser.add_argument('-d', '--decoder', default='mdf2parquet_decode', help='Path to the MF4 decoder executable')
    args = parser.parse_args()
    
    # Set environment variables
    os.environ["INPUT_BUCKET"] = args.input_bucket
    os.environ["MF4_DECODER"] = args.decoder
    
    # Call the container entry point
    return run_container()

if __name__ == "__main__":
    # Check if arguments were passed
    if len(sys.argv) > 1:
        # If arguments were provided, use them
        sys.exit(main())
    else:
        # Otherwise, assume environment variables are set and just run the container entry point
        sys.exit(run_container())
