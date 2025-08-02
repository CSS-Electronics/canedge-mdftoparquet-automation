#!/usr/bin/env python3
"""
Local test harness for process_backlog_amazon_entry.py

This script mocks the AWS Glue environment to test the process_backlog_amazon_entry.py script locally.
"""
import os
import sys
import logging
import zipfile
import tempfile
import shutil
from pathlib import Path

# Add the project root directory to Python path to allow importing from any module
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Mock the awsglue.utils.getResolvedOptions function
class MockGlue:
    @staticmethod
    def getResolvedOptions(argv, options):
        """Mock the Glue getResolvedOptions function"""
        # Return a dictionary with the test values for each option
        return {
            "input_bucket": "test-bucket",
            "lambda_zip_name": "test_lambda.zip",
            "decoder": "mock_decoder.exe" if os.name == 'nt' else "mock_decoder"
        }

# Add mock module to sys.modules
sys.modules['awsglue.utils'] = MockGlue()

# Mock boto3 S3 client
class MockS3Client:
    def download_file(self, bucket, key, filename):
        """Mock S3 download by creating a dummy zip file"""
        logger.info(f"Mock downloading from s3://{bucket}/{key} to {filename}")
        
        # Create a dummy zip file with test content
        with zipfile.ZipFile(filename, 'w') as zip_file:
            # Create a more realistic process_backlog_amazon.py file that includes the run_backlog_processing function
            zip_file.writestr('process_backlog_amazon.py', 
                             '#!/usr/bin/env python3\n'
                             'import os\n'
                             'import sys\n'
                             'import logging\n\n'
                             'logger = logging.getLogger()\n\n'
                             'def run_backlog_processing(input_bucket):\n'
                             '    logger.info(f"Mock processing backlog from bucket: {input_bucket}")\n'
                             '    print(f"Processing backlog from bucket: {input_bucket}")\n'
                             '    print(f"Using decoder at: {os.environ.get(\'MF4_DECODER\')}")\n'
                             '    return True\n\n'
                             'def main():\n'
                             '    input_bucket = os.environ.get("INPUT_BUCKET")\n'
                             '    logger.info("This is a mock backlog processing script")\n'
                             '    success = run_backlog_processing(input_bucket)\n'
                             '    return 0 if success else 1\n\n'
                             'if __name__ == "__main__":\n'
                             '    sys.exit(main())\n')
            
            # Create dummy modules directory with utils.py
            zip_file.writestr('modules/__init__.py', '')
            zip_file.writestr('modules/utils.py', 
                             'class ProcessBacklog:\n'
                             '    def __init__(self, cloud, client, bucket, logger):\n'
                             '        self.cloud = cloud\n'
                             '        self.client = client\n'
                             '        self.bucket = bucket\n'
                             '        self.logger = logger\n\n'
                             '    def process_backlog_from_cloud(self):\n'
                             '        print("Mock process_backlog_from_cloud executed successfully")\n'
                             '        return True\n')
            
            # Create a dummy decoder file
            decoder_content = 'echo "This is a mock decoder"' if os.name == 'nt' else '#!/bin/bash\necho "This is a mock decoder"'
            zip_file.writestr(
                'mock_decoder.exe' if os.name == 'nt' else 'mock_decoder', 
                decoder_content
            )
        
        return True

# Mock boto3 module
class MockBoto3:
    @staticmethod
    def client(service_name):
        """Return a mock client for the requested AWS service"""
        if service_name == 's3':
            return MockS3Client()
        raise NotImplementedError(f"Mock for {service_name} not implemented")

# Add mock module to sys.modules
sys.modules['boto3'] = MockBoto3()

def run_test():
    """Run the test harness"""
    logger.info("Starting local test of process_backlog_amazon_entry.py")
    
    try:
        # Import the path to the script we want to test
        backlog_script_path = os.path.join(PROJECT_ROOT, "mdftoparquet-backlog", "process_backlog_amazon_entry.py")
        logger.info(f"Testing script at: {backlog_script_path}")
        
        # Check if the file exists
        if not os.path.exists(backlog_script_path):
            logger.error(f"Script not found at {backlog_script_path}")
            return False
            
        # Import the script to test
        sys.path.insert(0, os.path.join(PROJECT_ROOT, "mdftoparquet-backlog"))
        import process_backlog_amazon_entry
        
        # Call the main function
        result = process_backlog_amazon_entry.main()
        
        # Check the result
        if result == 0:
            logger.info("Test completed successfully!")
            return True
        else:
            logger.error(f"Test failed with exit code {result}")
            return False
            
    except Exception as e:
        logger.error(f"Test failed with exception: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    run_test()
