#!/usr/bin/env python3
"""
Aggregation Local Runner

This script provides functionality for testing the AggregateData class locally.
It uses the local file system as the storage provider and relies on files in local-input-bucket
for the aggregations.json configuration and local-input-bucket-parquet for the Parquet files.

All date parameters and trip configuration are now read from the aggregations.json file.

Usage examples:
  python aggregation_local.py
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("aggregation_local")

# Get repository root and setup Python path
repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, repo_root)

# Import the AggregateData class from modules
from modules.aggregation import AggregateData

def setup_environment():
    """
    Configure environment variables for local testing
    
    Returns:
        tuple: (input_bucket_path, output_bucket_path, input_bucket, output_bucket)
    """
    # Set default bucket names for local testing
    input_bucket = os.environ.get('INPUT_BUCKET', 'local-input-bucket')
    output_bucket = os.environ.get('OUTPUT_BUCKET', f"{input_bucket}-parquet")
    
    # Get full paths to local bucket directories
    input_bucket_path = os.path.join(repo_root, 'local-testing', input_bucket)
    output_bucket_path = os.path.join(repo_root, 'local-testing', output_bucket)
    
    logger.info(f"Input bucket path: {input_bucket_path}")
    logger.info(f"Output bucket path: {output_bucket_path}")
    
    # Return both full paths and bucket names
    return (input_bucket_path, output_bucket_path)

def run_aggregation():
    input_bucket_path, output_bucket_path = setup_environment()
    
    try:
        aggregator = AggregateData(
            cloud="Local",
            client=None,
            input_bucket=input_bucket_path,
            output_bucket=output_bucket_path,
            logger=logger
        )
        
        aggregator.process_data_lake()
        return 0
        
    except Exception as e:
        logger.error(f"Error running aggregation: {e}")
        return 1

def main():
    success = run_aggregation()
    return success

if __name__ == "__main__":
    sys.exit(main())
