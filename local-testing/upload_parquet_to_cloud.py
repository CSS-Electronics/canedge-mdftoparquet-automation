#!/usr/bin/env python3
"""
Parquet Data Lake Uploader

This script uploads local Parquet data lake files to a cloud storage bucket (Amazon, Azure, or Google).
The folder structure of the Parquet files is preserved when uploading to the cloud.

Usage:
  python upload_parquet_to_cloud.py --cloud <Amazon|Azure|Google> [--local-folder <path>] [--output-bucket <bucket-name>]

Examples:
  python upload_parquet_to_cloud.py --cloud Amazon
  python upload_parquet_to_cloud.py --cloud Google --local-folder ./my-parquet-data
  python upload_parquet_to_cloud.py --cloud Azure --local-folder ./local-input-bucket-parquet --output-bucket my-output-bucket
"""

import os
import sys
import argparse
import logging
from pathlib import Path

# Add repository root and modules to Python path
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(script_dir)  # Parent directory of script directory
sys.path.insert(0, repo_root)
sys.path.insert(0, os.path.join(repo_root, 'modules'))
sys.path.insert(0, script_dir)  # Add local-testing directory as well

# Now import after fixing the path
import boto3
import google.cloud.storage
from azure.storage.blob import BlobServiceClient
from utils_testing import load_creds_file_into_env
from modules.cloud_functions import upload_object

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("upload_parquet_to_cloud")

def get_cloud_client(cloud):
    """
    Get the appropriate cloud storage client based on the cloud provider.
    
    Args:
        cloud (str): Cloud provider ("Amazon", "Azure", or "Google")
        
    Returns:
        object: The cloud storage client
    """
    if cloud == "Amazon":
        return boto3.client('s3')
    elif cloud == "Google":
        return google.cloud.storage.Client()
    elif cloud == "Azure":
        connection_string = os.environ.get("StorageConnectionString")
        if not connection_string:
            logger.error("Azure storage connection string not found in environment variables")
            return None
        return BlobServiceClient.from_connection_string(connection_string)
    else:
        logger.error(f"Unsupported cloud provider: {cloud}")
        return None

def find_parquet_files(local_folder):
    """
    Find all Parquet files in the given folder and its subdirectories.
    
    Args:
        local_folder (str): Path to the folder containing Parquet files
        
    Returns:
        list: List of tuples (full_path, relative_path) for each Parquet file
    """
    parquet_files = []
    folder_path = Path(local_folder)
    
    # Check if folder exists
    if not folder_path.exists():
        logger.error(f"Local folder {local_folder} does not exist")
        return parquet_files
    
    # Find all .parquet files
    for file_path in folder_path.glob('**/*.parquet'):
        # Get the relative path from the local folder
        rel_path = file_path.relative_to(folder_path)
        parquet_files.append((file_path, rel_path))
    
    logger.info(f"Found {len(parquet_files)} Parquet files in {local_folder}")
    return parquet_files

def upload_parquet_files(cloud, client, local_folder, output_bucket, parquet_files):
    """
    Upload Parquet files to cloud storage, preserving folder structure.
    
    Args:
        cloud (str): Cloud provider ("Amazon", "Azure", or "Google")
        client: Cloud storage client
        local_folder (str): Path to the local folder containing Parquet files
        output_bucket (str): Name of the output bucket/container
        parquet_files (list): List of tuples (full_path, relative_path) for each Parquet file
        
    Returns:
        tuple: (success_count, failure_count)
    """
    success_count = 0
    failure_count = 0
    
    for full_path, rel_path in parquet_files:
        # Normalize path separators for cloud storage
        object_path = str(rel_path).replace('\\', '/')
        local_path = str(full_path)
        
        logger.info(f"Uploading {local_path} to {output_bucket}/{object_path}")
        
        # Upload the file
        success = upload_object(cloud, client, output_bucket, object_path, local_path, logger)
        
        if success:
            success_count += 1
        else:
            failure_count += 1
    
    return success_count, failure_count

def main():
    """
    Parse command-line arguments and execute the upload process
    """
    parser = argparse.ArgumentParser(
        description='Upload local Parquet data lake files to a cloud storage bucket',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument('--cloud', 
                      choices=['Amazon', 'Azure', 'Google'],
                      required=True,
                      help='Cloud provider to use (Amazon, Azure, or Google)')
                      
    parser.add_argument('--local-folder', 
                      default='local-input-bucket-parquet',
                      help='Local folder containing the Parquet data lake (default: local-input-bucket-parquet)')
    
    parser.add_argument('--output-bucket', 
                      help='Name of the output bucket/container (default: <INPUT_BUCKET>-parquet from environment or credentials)')
    
    args = parser.parse_args()
    
    # Repository root is already in sys.path from the imports section
    # No need to add it again
    
    # Load cloud credentials
    if not load_creds_file_into_env(args.cloud, logger):
        logger.error(f"Failed to load credentials for {args.cloud}")
        return 1
    
    # Determine local folder path (relative to current directory if not absolute)
    local_folder = args.local_folder
    if not os.path.isabs(local_folder):
        local_folder = os.path.join(os.getcwd(), local_folder)
    
    # Determine output bucket name
    output_bucket = args.output_bucket
    if not output_bucket:
        # Try to get from environment variables
        output_bucket = os.environ.get('OUTPUT_BUCKET')
        if not output_bucket:
            # Try to infer from INPUT_BUCKET if available
            input_bucket = os.environ.get('INPUT_BUCKET')
            if input_bucket:
                output_bucket = f"{input_bucket}-parquet"
            else:
                # For Google Cloud, try to get project_id and default bucket name
                if args.cloud == "Google":
                    try:
                        # Try to read from Google credentials
                        creds_path = os.path.join(script_dir, "creds", "google-creds.json")
                        if os.path.exists(creds_path):
                            with open(creds_path, 'r') as f:
                                import json
                                creds_data = json.load(f)
                                project_id = creds_data.get('project_id')
                                if project_id:
                                    # Use standard naming convention for Google buckets
                                    output_bucket = f"{project_id}-parquet"
                                    logger.info(f"Inferred output bucket from Google credentials: {output_bucket}")
                    except Exception as e:
                        logger.warning(f"Failed to read Google credentials: {e}")
                
                # If still no output bucket, prompt user
                if not output_bucket:
                    logger.error("Output bucket not specified and could not be inferred")
                    logger.error("Please specify output bucket with --output-bucket parameter")
                    return 1
    
    logger.info(f"Using cloud provider: {args.cloud}")
    logger.info(f"Local Parquet folder: {local_folder}")
    logger.info(f"Output bucket: {output_bucket}")
    
    # Get the cloud client
    client = get_cloud_client(args.cloud)
    if not client:
        logger.error("Failed to initialize cloud client")
        return 1
    
    # Find Parquet files
    parquet_files = find_parquet_files(local_folder)
    if not parquet_files:
        logger.error(f"No Parquet files found in {local_folder}")
        return 1
    
    # Upload files
    logger.info(f"Starting upload of {len(parquet_files)} files to {args.cloud} storage...")
    success_count, failure_count = upload_parquet_files(
        args.cloud, client, local_folder, output_bucket, parquet_files
    )
    
    # Report results
    logger.info(f"\nUpload summary:")
    logger.info(f"- Total files: {len(parquet_files)}")
    logger.info(f"- Successfully uploaded: {success_count}")
    logger.info(f"- Failed to upload: {failure_count}")
    
    if failure_count > 0:
        logger.warning("Some files failed to upload. Check the logs for details.")
        return 1
    else:
        logger.info("All files uploaded successfully.")
        return 0

if __name__ == "__main__":
    sys.exit(main())
