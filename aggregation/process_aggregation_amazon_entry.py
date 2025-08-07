#!/usr/bin/env python3
"""
Amazon S3 Aggregation Entry Script for AWS Glue Spark

This script serves as the entry point for AWS Glue Spark jobs (version 5.0 with Python 3.11) to aggregate Parquet files to trip summary level.
It downloads the Lambda function ZIP archive containing the actual processing script and required modules,
extracts it to a secure temporary directory, and then executes the main aggregation script.

Required job parameters:
- input_bucket: S3 bucket containing the MDF files to process
- lambda_zip_name: Name of the Lambda ZIP file in the INPUT_BUCKET to download and extract
"""
# Standard imports
import os
import sys
import logging
import boto3
import zipfile
import tempfile
from pathlib import Path
import subprocess

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()
 
def process():
    logger.info(f"PREPARING FOR AGGREGATION\n\n")
    # extract parameters depending on environment and run job
    try:
        if 'GLUE_PYTHON_VERSION' in os.environ:
            from awsglue.utils import getResolvedOptions
            args = getResolvedOptions(sys.argv, ['input_bucket', 'lambda_zip_name'])
            input_bucket = args['input_bucket']
            lambda_zip_name = args['lambda_zip_name']
        else:
            input_bucket = os.environ.get("INPUT_BUCKET", "test-bucket")
            lambda_zip_name = os.environ.get("LAMBDA_ZIP_NAME", "test_lambda.zip")
        
        logger.info(f"Input bucket: {input_bucket}")
        logger.info(f"Lambda ZIP name: {lambda_zip_name}")
        
        # Create a secure temporary directory for our operations using context manager
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir_path = Path(temp_dir)
            extract_dir = temp_dir_path / "extract"
            extract_dir.mkdir(exist_ok=True)
            
            logger.info(f"Created temporary directory: {temp_dir} with extract subfolder: {extract_dir}")
            
            # Set up paths with secure random names
            local_zip = temp_dir_path / lambda_zip_name
        
            # Download the Lambda zip file from S3
            logger.info(f"Downloading Lambda ZIP from s3://{input_bucket}/{lambda_zip_name}")
            s3_client = boto3.client('s3')
            s3_client.download_file(input_bucket, lambda_zip_name, str(local_zip))
            
            # Extract the ZIP file
            logger.info(f"Extracting ZIP to {extract_dir}")
            with zipfile.ZipFile(local_zip, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            
            # Set up environment variables for the aggregation script
            os.environ["INPUT_BUCKET"] = input_bucket
                            
            # Run the aggregation script directly using subprocess
            logger.info("STARTING AGGREGATION")
            script_path = Path(extract_dir) / "process_aggregation_amazon.py"
            result = subprocess.run([sys.executable, str(script_path)], cwd=str(extract_dir))
            
            return result.returncode
    except Exception as e:
        logger.error(f"Error in aggregation entry script: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    process()
