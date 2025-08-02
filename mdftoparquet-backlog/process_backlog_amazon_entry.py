#!/usr/bin/env python3
"""
Amazon S3 Backlog Processing Entry Script for AWS Glue Spark

This script serves as the entry point for AWS Glue Spark jobs (version 5.0 with Python 3.11) to process MDF files to Parquet format.
It downloads the Lambda function ZIP archive containing the actual processing script and required modules,
extracts it to a secure temporary directory, and then executes the main backlog processing script.

Required job parameters:
- input_bucket: S3 bucket containing the MDF files to process
- lambda_zip_name: Name of the Lambda ZIP file in the INPUT_BUCKET to download and extract
- decoder: Name of the MF4 decoder executable (e.g., 'mdf2parquet_decode')
"""
# Standard imports
import os
import sys
import logging
import boto3
import zipfile
import subprocess
import tempfile
import shutil
from pathlib import Path

# Configure the root logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()

# Detect environment - we need different imports and setup for AWS Glue vs local testing
IN_GLUE_ENV = 'GLUE_PYTHON_VERSION' in os.environ

# Import AWS Glue modules conditionally
if IN_GLUE_ENV:
    # Glue Spark imports - only run in AWS Glue environment
    try:
        from pyspark.context import SparkContext
        from awsglue.context import GlueContext
        from awsglue.job import Job
        # We'll import getResolvedOptions inside the function where it's used to avoid scoping issues
        
        # Initialize Spark context
        sc = SparkContext()
        glueContext = GlueContext(sc)
        logger.info("Running in AWS Glue Spark environment")
    except ImportError:
        logger.error("Failed to import Glue modules in Glue environment")
        raise
else:
    # For local testing, we'll mock/import only what's needed
    logger.info("Running in local testing environment")

def process():
    """Main processing function for the job - works in both AWS Glue and local testing"""
    # Create temporary directories that will be cleaned up automatically
    temp_dir = None
    extract_dir = None
    job = None
    
    try:
        # Get parameters - different approach based on environment
        if IN_GLUE_ENV:
            # Import getResolvedOptions here inside the function to avoid scoping issues
            from awsglue.utils import getResolvedOptions
            
            # Get parameters from Glue job (must include 'JOB_NAME' for Spark jobs)
            args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_bucket', 'lambda_zip_name', 'decoder'])
            input_bucket = args['input_bucket']
            lambda_zip_name = args['lambda_zip_name']
            decoder_name = args['decoder']
            
            # Initialize the Glue job
            job = Job(glueContext)
            job.init(args['JOB_NAME'], args)
        else:
            # For local testing, parameters come directly from environment variables
            # This is more reliable and compatible with our existing test harness
            input_bucket = os.environ.get("INPUT_BUCKET", "test-bucket")
            lambda_zip_name = os.environ.get("LAMBDA_ZIP_NAME", "test_lambda.zip")
            decoder_name = os.environ.get("MF4_DECODER", "mock_decoder.exe")
        
        # Log the parameters
        logger.info(f"Input bucket: {input_bucket}")
        logger.info(f"Lambda ZIP name: {lambda_zip_name}")
        logger.info(f"Decoder name: {decoder_name}")
        
        # Create a secure temporary directory for our operations
        temp_dir = tempfile.mkdtemp(prefix="lambda_processing_")
        
        # Set up paths with secure random names
        local_zip = Path(temp_dir) / "lambda_function.zip"
        extract_dir = tempfile.mkdtemp(prefix="lambda_extract_", dir=temp_dir)
        
        logger.info(f"Created temporary directories:\n - Temp dir: {temp_dir}\n - Extract dir: {extract_dir}")
        
        # Download the Lambda zip file from S3
        logger.info(f"Downloading Lambda ZIP from s3://{input_bucket}/{lambda_zip_name}")
        s3_client = boto3.client('s3')
        s3_client.download_file(input_bucket, lambda_zip_name, str(local_zip))
        
        # Extract the ZIP file
        logger.info(f"Extracting ZIP to {extract_dir}")
        with zipfile.ZipFile(local_zip, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        
        # Set up environment variables for the backlog script
        os.environ["INPUT_BUCKET"] = input_bucket
        decoder_path = Path(extract_dir) / decoder_name
        os.environ["MF4_DECODER"] = str(decoder_path)
        
        # Make decoder executable if needed
        if not os.name == 'nt':  # Skip on Windows
            try:
                decoder_path.chmod(decoder_path.stat().st_mode | 0o111)  # Add executable permission
                logger.info(f"Made decoder executable: {decoder_path}")
            except Exception as e:
                logger.warning(f"Could not make decoder executable: {e}")
        
        # Add the extracted path to Python's path
        sys.path.insert(0, str(extract_dir))
        
        # Run the backlog processing script
        logger.info("Starting backlog processing script")
        script_path = Path(extract_dir) / "process_backlog_amazon.py"
        
        # First, get the directory containing the script and ensure it's in sys.path
        script_dir = str(script_path.parent)
        if script_dir not in sys.path:
            sys.path.insert(0, script_dir)
        
        # Import the script as a module (without the .py extension)
        import importlib.util
        spec = importlib.util.spec_from_file_location("process_backlog_amazon", str(script_path))
        backlog_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(backlog_module)
        
        # Now call the main function from the imported module
        result = backlog_module.main()
        
        if result == 0:
            logger.info("Backlog processing completed successfully")
            # Signal successful job completion for Spark job (only in Glue environment)
            if IN_GLUE_ENV and job is not None:
                job.commit()
                logger.info("Glue job committed successfully")
            return 0
        else:
            logger.error(f"Backlog processing failed with exit code: {result}")
            return result
    except Exception as e:
        logger.error(f"Error in backlog processing entry script: {str(e)}", exc_info=True)
        return 1
    finally:
        # Clean up temporary directories
        try:
            if extract_dir and os.path.exists(extract_dir):
                logger.info(f"Cleaning up extract directory: {extract_dir}")
                shutil.rmtree(extract_dir, ignore_errors=True)
            
            if temp_dir and os.path.exists(temp_dir):
                logger.info(f"Cleaning up temporary directory: {temp_dir}")
                shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception as cleanup_error:
            logger.warning(f"Error during cleanup: {cleanup_error}")
            # Continue execution, don't fail on cleanup errors

def main():
    # Call the process function and return the result
    return process()

if __name__ == "__main__":
    # Run the process function and handle exit appropriately for each environment
    result = process()
    
    # In local environment, return the proper exit code
    # In Glue environment, the job.commit() is handled in process()
    if not IN_GLUE_ENV:
        sys.exit(result)
