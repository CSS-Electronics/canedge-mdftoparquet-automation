#!/usr/bin/env python3
# Amazon S3 Aggregation Script
# 
# This script processes an aggregations.json file from an input bucket to aggregate Parquet data to trip summary level.
#
# Required environment variables:
# - INPUT_BUCKET: S3 bucket containing the MDF files to process

import os
import sys
import logging
import boto3
from modules.aggregation import AggregateData

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()

def run_aggregation():
    input_bucket = os.environ.get("INPUT_BUCKET") 
    output_bucket = input_bucket + "-parquet"
    cloud = "Amazon"
    try:
        storage_client = boto3.client('s3')
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
    sys.exit(run_aggregation())
