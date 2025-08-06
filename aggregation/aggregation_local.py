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
        tuple: (input_bucket_path, output_bucket_path, input_bucket_name, output_bucket_name)
    """
    # Set default bucket names for local testing
    input_bucket_name = os.environ.get('INPUT_BUCKET', 'local-input-bucket')
    output_bucket_name = os.environ.get('OUTPUT_BUCKET', f"{input_bucket_name}-parquet")
    
    # Set these in the environment for other scripts to use
    os.environ['INPUT_BUCKET'] = input_bucket_name
    os.environ['OUTPUT_BUCKET'] = output_bucket_name
    
    # Get full paths to local bucket directories
    input_bucket_path = os.path.join(repo_root, 'local-testing', input_bucket_name)
    output_bucket_path = os.path.join(repo_root, 'local-testing', output_bucket_name)
    
    # Ensure bucket directories exist
    for path in [input_bucket_path, output_bucket_path]:
        if not os.path.exists(path):
            logger.error(f"Path does not exist: {path}")
            sys.exit()
    
    logger.info(f"Input bucket path: {input_bucket_path}")
    logger.info(f"Output bucket path: {output_bucket_path}")
    
    # Return both full paths and bucket names
    return (input_bucket_path, output_bucket_path, input_bucket_name, output_bucket_name)

def run_aggregation():
    """
    Run the AggregateData class for local testing
    
    All configuration parameters including date ranges and trip settings
    are now read from the aggregations.json file.
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Set up environment and get bucket paths and names
    input_bucket_path, output_bucket_path, input_bucket_name, output_bucket_name = setup_environment()
    
    # Check if aggregations.json exists in the input bucket
    aggregations_file = "aggregations.json"
    aggregations_path = os.path.join(input_bucket_path, aggregations_file)
    
    if not os.path.exists(aggregations_path):
        logger.error(f"Aggregations file not found: {aggregations_path}")
        logger.error("Please ensure aggregations.json is placed in the local-input-bucket directory")
        return False
    
    logger.info(f"Using input bucket path: {input_bucket_path}")
    logger.info(f"Using output bucket path: {output_bucket_path}")
    
    # Initialize AggregateData class
    try:
        # For Local, we pass None as client since cloud_functions handles local operations
        aggregator = AggregateData(
            cloud="Local",
            client=None,
            input_bucket=input_bucket_path,
            output_bucket=output_bucket_path,
            logger=logger
        )
        
        # Load aggregation configuration
        config = aggregator.load_aggregation_json()
        if not config:
            logger.error("Failed to load aggregation configuration")
            return False
    
        logger.info(config)
        
        # Process the data
        logger.info("Starting data processing with configuration")
        # Access config values directly instead of attributes
        date_config = config.get('config', {}).get('date', {})
        date_mode = date_config.get('mode')
        start_date = date_config.get('start_date')
        end_date = date_config.get('end_date')
        trip_config = config.get('config', {}).get('trip', {})
        trip_gap = trip_config.get('trip_gap_min')
        trip_min_length = trip_config.get('trip_min_length_min')
        
        logger.info(f"Date mode from config: {date_mode}")
        logger.info(f"Start date from config: {start_date}")
        logger.info(f"End date from config: {end_date}")
        logger.info(f"Trip gap minutes from config: {trip_gap}")
        logger.info(f"Trip min length minutes from config: {trip_min_length}")
        logger.info(f"Clusters: {config.get('device_clusters', [])}")
        
        # Also log internal aggregator attributes for validation
        logger.info(f"Aggregator start_date: {aggregator.start_date}")
        logger.info(f"Aggregator end_date: {aggregator.end_date}")
        logger.info(f"Aggregator trip_gap_min: {aggregator.trip_gap_min}")
        logger.info(f"Aggregator trip_min_length_min: {aggregator.trip_min_length_min}")
        
        # Run data processing
        days_processed = aggregator.process_data_lake(config)
        
        # Check if any days were processed
        logger.info(f"Days processed: {days_processed}")
        
        # Check if any aggregation files were created
        aggregation_dir = os.path.join(output_bucket_path, "aggregations")
        if os.path.exists(aggregation_dir):
            logger.info(f"Aggregation directory exists: {aggregation_dir}")
            for root, dirs, files in os.walk(aggregation_dir):
                for file in files:
                    logger.info(f"Found aggregation file: {os.path.join(root, file)}")
        else:
            logger.info(f"Aggregation directory does not exist: {aggregation_dir}")
            
        logger.info("\n✅ Aggregation process completed successfully\n")
        return True
        
    except Exception as e:
        logger.error(f"Error running aggregation: {e}")
        return False

def main():
    """
    Run the aggregation process using configuration from aggregations.json
    """
    logger.info("Starting aggregation process using configuration from aggregations.json")
    logger.info("Date ranges and trip parameters will be read from the configuration file")
    
    # Run aggregation
    success = run_aggregation()
    
    if not success:
        logger.error("\n❌ Aggregation failed\n")
        sys.exit(1)
    else:
        logger.info("\n✅ Aggregation successful\n")
        sys.exit(0)

if __name__ == "__main__":
    sys.exit(main())
