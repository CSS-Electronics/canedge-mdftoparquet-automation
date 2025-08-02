#!/usr/bin/env python3
"""
Local backlog processing script for converting MDF files to Parquet without cloud storage.
This script processes MDF files from a local input folder and converts them to Parquet format
in a local output folder.
"""

import os
import sys
import logging
import argparse
from pathlib import Path

# Add the parent directory to the path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import utility modules
from modules.utils import ProcessBacklog

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Process a local backlog of MDF files')
    parser.add_argument('--input-folder', type=str, required=True, help='Local folder containing MDF files')
    args = parser.parse_args()
    
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger()
    
    # Get input folder from command-line argument
    input_folder = args.input_folder
    
    # Validate input folder
    input_path = Path(input_folder)
    if not input_path.exists() or not input_path.is_dir():
        logger.error(f"Input folder '{input_folder}' does not exist or is not a directory")
        sys.exit(1)
    
    # Setup MF4 decoder environment variable if not set
    if 'MF4_DECODER' not in os.environ:
        # Default to local-testing\mdf4decoder.exe
        mf4_decoder = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                                 "local-testing", "mdf4decoder.exe")
        if os.path.exists(mf4_decoder):
            os.environ['MF4_DECODER'] = mf4_decoder
            logger.info(f"MF4_DECODER set to {mf4_decoder}")
        else:
            logger.error(f"MF4 decoder not found at {mf4_decoder}")
            logger.error("Please set the MF4_DECODER environment variable")
            sys.exit(1)
    
    # Set environment variables for local processing
    os.environ['INPUT_BUCKET'] = str(input_path)
    
    # Define output folder (input folder name + "-parquet")
    output_folder = f"{input_folder}-parquet"
    os.makedirs(output_folder, exist_ok=True)
    
    # Initialize ProcessBacklog for local processing
    # No storage client needed for local processing
    process_backlog = ProcessBacklog("Local", None, input_folder, logger)
    
    # Add DBC files to valid extensions
    process_backlog.valid_extensions.append(".DBC")
    
    # Process the backlog
    logger.info(f"Processing backlog from {input_folder}")
    result = process_backlog.process_backlog_from_cloud()
    
    # Check result
    if result:
        logger.info(f"Backlog processing completed successfully. Output saved to {output_folder}")
    else:
        logger.error(f"Backlog processing failed")
        sys.exit(1)

if __name__ == "__main__":
    main()
