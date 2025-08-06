#!/usr/bin/env python3
"""
MDF to Parquet and Aggregation Local Test Runner

This script provides functionality for testing MDF to Parquet conversion and aggregation locally for
different cloud providers (Amazon, Azure, Google). It handles single file processing,
backlog processing, and data aggregation scenarios.

For aggregation tests, all date parameters and trip configuration are read from the aggregations.json
file located in the input bucket directory.

Usage examples:
  Single object test: python run_test.py --cloud Amazon --input-bucket test-bucket --object-path myfile.MF4
  Backlog test:      python run_test.py --cloud Amazon --input-bucket test-bucket --backlog
  Aggregation test:  python run_test.py --cloud Amazon --input-bucket test-bucket --aggregate
"""

import os
import sys
import argparse
import logging
import subprocess
from datetime import datetime, timedelta
from utils_testing import load_creds_file_into_env, create_cloud_event

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("run_test")

def setup_environment(cloud, input_bucket, decoder_path=None):
    """Configure Python path and environment variables for testing
    
    Args:
        cloud: Cloud provider (Amazon, Azure, Google)
        input_bucket: Name of the input bucket/container
        decoder_path: Path to the MF4 decoder executable (optional)
    
    Returns:
        repo_root: Path to the repository root directory
    """
    # Get repository root and setup Python path
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Add all necessary directories to Python path
    sys.path.insert(0, repo_root)
    sys.path.insert(0, os.path.join(repo_root, "mdftoparquet"))
    sys.path.insert(0, os.path.join(repo_root, "mdftoparquet-backlog"))
    sys.path.insert(0, os.path.join(repo_root, "local-testing"))
    sys.path.insert(0, os.path.join(repo_root, "modules"))
    
    # Update PYTHONPATH environment variable
    os.environ["PYTHONPATH"] = f"{repo_root};{os.environ.get('PYTHONPATH', '')}"
    
    # Set up environment variables
    # Check if input bucket is specified
    if not input_bucket:
        # Check if it's in environment variables
        input_bucket = os.environ.get('INPUT_BUCKET')
        
    # Fail if we still don't have an input bucket
    if not input_bucket:
        logger.error("No input bucket specified. This is required.")
        return None
        
    # For Azure specifically, rename the connection string env var to match what container script expects
    if cloud == "Azure" and os.environ.get("AZURE_STORAGE_CONNECTION_STRING"):
        os.environ["StorageConnectionString"] = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        logger.info("Azure storage connection string mapped to StorageConnectionString")
    
    # Set output bucket based on input bucket if not already in environment
    output_bucket = os.environ.get('OUTPUT_BUCKET')
    if not output_bucket:
        output_bucket = f"{input_bucket}-parquet"

    # Set these in the environment for other scripts to use
    os.environ['INPUT_BUCKET'] = input_bucket
    os.environ['OUTPUT_BUCKET'] = output_bucket
    
    # Log the actual bucket names being used
    logger.info(f"Using INPUT_BUCKET: {input_bucket}")
    logger.info(f"Using OUTPUT_BUCKET: {output_bucket}")
    
    # Set decoder path if provided, otherwise use default
    if not decoder_path or decoder_path == "mdf2parquet_decode.exe":
        # If default decoder is used, get the absolute path from repo root
        decoder_path = os.path.join(repo_root, "mdf2parquet_decode.exe")
        logger.debug(f"Using default decoder path: {decoder_path}")
    
    # Ensure the decoder path is absolute
    if not os.path.isabs(decoder_path):
        decoder_path = os.path.join(repo_root, decoder_path)
    
    # Set the environment variable
    os.environ["MF4_DECODER"] = decoder_path
    logger.info(f"Using decoder: {decoder_path}")
    
    # Load cloud-specific credentials (not needed for Local)
    if cloud.lower() != "local":
        if not load_creds_file_into_env(cloud, logger):
            logger.error(f"Failed to load credentials for {cloud}")
            return None
        
    logger.debug(f"Environment configured for {cloud}")
    logger.debug(f"Python path: {sys.path[:5]}")
    logger.debug(f"MF4_DECODER: {os.environ.get('MF4_DECODER')}")
    logger.debug(f"INPUT_BUCKET: {os.environ.get('INPUT_BUCKET')}")
    
    return repo_root

def process_cloud_function(cloud, event):
    """Execute cloud function code locally with the provided event
    
    Args:
        cloud: Cloud provider (Amazon, Azure, Google, Local)
        event: Cloud event object or mock event
        
    Returns:
        bool: True if the function executed successfully, False otherwise
    """
    logger.info(f"Processing single file with {cloud} function")
    
    try:
        # Import and run the appropriate cloud function
        if cloud == "Amazon":
            import lambda_function
            return lambda_function.lambda_handler(event, {}) is not None
        elif cloud == "Azure":
            import function_app
            function_app.MdfToParquet(event)
            return True
        elif cloud == "Google":
            import main
            main.process_mdf_file(event)
            return True
        elif cloud == "Local":
            import local_function
            local_function.process_mdf_file(event)
            return True
        else:
            logger.error(f"Unsupported cloud provider: {cloud}")
            return False
    except Exception as e:
        logger.error(f"Error executing {cloud} function: {e}")
        return False
        
def process_backlog(cloud, input_bucket):
    """Execute backlog processing for the specified cloud provider
    
    Args:
        cloud: Cloud provider (Amazon, Azure, Google, Local)
        input_bucket: Name of the input bucket/container
        
    Returns:
        bool: True if the backlog processing executed successfully, False otherwise
    """
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # For Azure, use the container script (process_backlog_container.py)
    if cloud == "Azure":
        backlog_script_path = os.path.join(repo_root, "mdftoparquet-backlog", "process_backlog_container.py")
        # Set the CLOUD environment variable for the container script
        os.environ["CLOUD"] = "Azure"
    else:
        # For other cloud providers, use their specific scripts
        backlog_script_path = os.path.join(repo_root, "mdftoparquet-backlog", f"process_backlog_{cloud.lower()}.py")
    
    logger.info(f"Processing backlog using {backlog_script_path}")
    
    # Prepare command based on cloud provider
    if cloud in ["Azure", "Google", "Amazon"]:
        # Azure, Google, and Amazon scripts use environment variables
        cmd = [sys.executable, backlog_script_path]
    elif cloud == "Local":
        cmd = [sys.executable, backlog_script_path, '--input-folder', input_bucket]
    
    # Run the backlog script as a subprocess
    try:
        logger.info(f"Executing: {' '.join(cmd)}")
        process = subprocess.run(cmd, check=True, env=os.environ)
        return process.returncode == 0
    except subprocess.CalledProcessError as e:
        logger.error(f"Error executing backlog script: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False
        
def process_aggregation(cloud, input_bucket):
    """Execute data aggregation for the specified cloud provider
    
    Args:
        cloud: Cloud provider (Amazon, Azure, Google, Local)
        input_bucket: Name of the input bucket/container
        
    Returns:
        bool: True if the aggregation executed successfully, False otherwise
    """
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    agg_script_path = os.path.join(repo_root, "aggregation", "aggregation_local.py")
    
    # Check if the aggregation script exists
    if not os.path.exists(agg_script_path):
        logger.error(f"Aggregation script not found at {agg_script_path}")
        return False
        
    # Execute the aggregation script
    try:
        logger.info(f"Executing: {sys.executable} {agg_script_path}")
        result = subprocess.run([sys.executable, agg_script_path])
        
        if result.returncode == 0:
            logger.info("\n\u2705 Local aggregation test completed successfully\n")
            return True
        else:
            if result.stderr.strip():
                logger.error(f"Aggregation script error output:\n{result.stderr}")
            logger.error(f"Aggregation script failed with return code {result.returncode}")
            logger.error("\n\u274c Test failed\n")
            return False
    except Exception as e:
        logger.error(f"Error executing aggregation script: {e}")
        logger.error("\n\u274c Test failed\n")
        return False


def run_test(cloud, input_bucket, object_path=None, aggregate_flag=False, process_backlog_flag=False, 
          decoder_path=None):
    """
    Run the appropriate test based on the flags
    
    Args:
        cloud: Cloud provider (Amazon, Azure, Google, Local)
        input_bucket: Name of the input bucket/container
        object_path: Path to the object to process (for single file test)
        aggregate_flag: Flag to run aggregation test
        process_backlog_flag: Flag to run backlog test
        decoder_path: Path to the MF4 decoder executable
        
    Returns:
        bool: True if the test executed successfully, False otherwise
    """
    # Set up the environment for testing
    if not setup_environment(cloud, input_bucket, decoder_path):
        return False
    
    if aggregate_flag:
        # Process aggregation for the specified cloud provider
        logger.info(f"Processing aggregation for {cloud} from bucket: {input_bucket}")
        return process_aggregation(cloud, input_bucket)
    elif process_backlog_flag:
        # Process backlog for the specified cloud provider
        logger.info(f"Processing backlog for {cloud} from bucket: {input_bucket}")
        return process_backlog(cloud, input_bucket)
    else:
        # Create and process a cloud event for a single file
        if not object_path:
            logger.error("Object path is required for single file tests")
            return False
            
        event = create_cloud_event(cloud, object_path, input_bucket)
        logger.info(f"Using event: {event}")
        
        return process_cloud_function(cloud, event)

def main():
    """
    Parse command-line arguments and run the appropriate test
    """
    parser = argparse.ArgumentParser(
        description='Run local tests for cloud functions using either a single object, a backlog file, or aggregation',
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument('-c', '--cloud', 
                        choices=['Amazon', 'Azure', 'Google', 'Local'], 
                        required=True,
                        help='Cloud provider to use (Amazon, Azure, Google, or Local)')
                        
    parser.add_argument('-i', '--input-bucket', 
                        required=True,
                        help='Input bucket/container name (required for all operations)')
                        
    # Create mutually exclusive group for operation type
    operation_group = parser.add_mutually_exclusive_group()
                        
    operation_group.add_argument('-b', '--backlog', 
                        action='store_true',
                        help='Process backlog.json from the cloud storage input bucket')
    
    operation_group.add_argument('-a', '--aggregate',
                        action='store_true',
                        help='Run data aggregation using the AggregateData class')
                        
    parser.add_argument('-o', '--object-path', 
                        default='',
                        help='Object path to use for single file testing (required when neither --backlog nor --aggregate is provided)')
                        
    parser.add_argument('-d', '--decoder', 
                        default='mdf2parquet_decode.exe',
                        help='Path to the MF4 decoder executable (default: mdf2parquet_decode.exe in repo root)')
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.backlog and not args.aggregate and not args.object_path:
        parser.error('--object-path is required when neither --backlog nor --aggregate is provided')
    
    # Run the test
    test_type = "aggregation" if args.aggregate else "backlog" if args.backlog else "single file"
    logger.info(f"\nRunning local {test_type} test for {args.cloud}\n")
    
    result = run_test(
        cloud=args.cloud,
        input_bucket=args.input_bucket,
        object_path=args.object_path,
        aggregate_flag=args.aggregate,
        process_backlog_flag=args.backlog,
        decoder_path=args.decoder
    )
    
    # Report results
    if result:
        logger.info("\n\n✅ Test completed successfully\n")
        return 0
    else:
        logger.error("\n\n❌ Test failed\n")
        return 1

if __name__ == "__main__":
    sys.exit(main())
