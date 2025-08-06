#!/usr/bin/env python3
# Amazon S3 Backlog Processing Script
# 
# This script processes a backlog.json file from an S3 bucket to convert MDF files to Parquet format.
# The backlog.json should be located in the root of the input S3 bucket.
#
# Required environment variables:
# - INPUT_BUCKET: S3 bucket containing the MDF files to process
# - MF4_DECODER: Path to the MF4 decoder executable
import os
import sys
import logging
import boto3
from modules.utils import ProcessBacklog

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()

def run_backlog_processing():
    input_bucket = os.environ.get("INPUT_BUCKET") 
    cloud = "Amazon"
    try:
        storage_client = boto3.client('s3')
        pb = ProcessBacklog(cloud, storage_client, input_bucket, logger)
        pb.process_backlog_from_cloud()
        return 0
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(run_backlog_processing())
