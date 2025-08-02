#!/usr/bin/env python3
# Google Cloud Storage Backlog Processing Script
# 
# This script processes a backlog.json file from a Google Cloud Storage bucket to convert MDF files to Parquet format.
# The backlog.json should be located in the root of the input bucket.
#
# This is implemented as an HTTP-triggered Cloud Function.
import os
import sys
import logging
import functions_framework
from google.cloud import storage
from modules.utils import ProcessBacklog


# Configure the root logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()

def run_backlog_processing(input_bucket):
    """Core backlog processing logic used by both HTTP handler and main function"""
    # Initialize Google Cloud Storage client
    try:
        storage_client = storage.Client()
        
        # Process the backlog
        logger.info(f"Starting backlog processing for bucket: {input_bucket}")
        pb = ProcessBacklog("Google", storage_client, input_bucket, logger)
        success = pb.process_backlog_from_cloud()
        
        if success:
            logger.info("Backlog processing completed successfully")
            return True
        else:
            logger.error("Backlog processing failed")
            return False
            
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return False

@functions_framework.http
def process_mdf_file(request):
    """HTTP Cloud Function entry point - matches Terraform configuration"""
    # Get environment variables - assume INPUT_BUCKET is always provided
    input_bucket = os.environ.get('INPUT_BUCKET')
    
    # Run the backlog processing
    success = run_backlog_processing(input_bucket)
    
    if success:
        return {"status": "success", "message": "Backlog processing completed successfully"}, 200
    else:
        return {"status": "error", "message": "Backlog processing failed"}, 500

def main():
    """Command-line entry point for local invocation"""
    # Get input bucket directly from environment variables
    # Assume these are set by the cloud environment or local_invocation.py
    input_bucket = os.environ.get("INPUT_BUCKET")
    
    # Run the backlog processing
    success = run_backlog_processing(input_bucket)
    
    # Return appropriate exit code
    return 0 if success else 1
        
if __name__ == "__main__":
    # Just run the main function since it already uses environment variables
    sys.exit(main())
