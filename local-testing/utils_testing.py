#!/usr/bin/env python3
"""
Utility functions for testing cloud functions locally.
Contains functions for credential loading and management.
"""

import os
import json
import logging
from pathlib import Path


def load_credentials(creds_file_path):
    """
    Load credentials from a JSON file and add them to environment variables.
    
    Args:
        creds_file_path (str): Path to the JSON credentials file
        
    Returns:
        bool: True if credentials were successfully loaded, False otherwise
    """
    try:
        if not os.path.exists(creds_file_path):
            logging.error(f"Credentials file not found: {creds_file_path}")
            return False
            
        with open(creds_file_path, 'r') as f:
            creds = json.load(f)
            
        # Add all credentials to environment variables
        for key, value in creds.items():
            os.environ[key] = str(value)
            
        logging.info(f"Successfully loaded credentials from {creds_file_path}")
        return True
    except Exception as e:
        logging.error(f"Error loading credentials from {creds_file_path}: {e}")
        return False


def load_creds_file_into_env(cloud, logger=None):
    """
    Load cloud-specific credentials from file into environment variables.
    
    Args:
        cloud (str): Cloud provider ("Amazon", "Azure", or "Google")
        logger: Optional logger object for logging messages
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Use print if logger is not provided
    log = logger.info if logger else print
    error = logger.error if logger else print
    
    # Determine credentials file path
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    creds_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "creds")
    creds_file = f"{cloud.lower()}-creds.json"
    creds_file_path = os.path.join(creds_dir, creds_file)
    
    # Check if credentials file exists
    if not os.path.exists(creds_file_path):
        error(f"Credentials file not found: {creds_file_path}")
        return False
    
    try:
        # Load the credentials file
        log(f"Loading credentials from {creds_file}")
        with open(creds_file_path, 'r') as f:
            creds_json = json.load(f)
        
        # Set cloud-specific environment variables
        if cloud == "Google":
            # Set Google application credentials path
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_file_path
            log(f"Set GOOGLE_APPLICATION_CREDENTIALS={creds_file_path}")
            
            # Set Google Cloud project ID
            if 'project_id' in creds_json:
                os.environ["GCLOUD_PROJECT"] = creds_json['project_id']
                log(f"Set GCLOUD_PROJECT={creds_json['project_id']}")
            else:
                error("Google credentials file does not contain project_id")
                return False
                
        elif cloud == "Azure":
            # Set Azure storage connection string
            if 'STORAGE_CONNECTION_STRING' in creds_json:
                os.environ["StorageConnectionString"] = creds_json['STORAGE_CONNECTION_STRING']
                log("Set StorageConnectionString from credentials file")
            else:
                error("Azure credentials file does not contain STORAGE_CONNECTION_STRING")
                return False
                
        elif cloud == "Amazon":
            # AWS credentials are always uppercase as per user requirement
            required_keys = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
            missing_keys = [key for key in required_keys if key not in creds_json]
            
            if missing_keys:
                error(f"AWS credentials file missing required keys: {', '.join(missing_keys)}")
                return False
                
            # Set AWS credentials to environment variables
            os.environ["AWS_ACCESS_KEY_ID"] = creds_json['AWS_ACCESS_KEY_ID']
            os.environ["AWS_SECRET_ACCESS_KEY"] = creds_json['AWS_SECRET_ACCESS_KEY']
            log("Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY from credentials file")
                        
            # Also set region if available
            if 'AWS_DEFAULT_REGION' in creds_json:
                os.environ["AWS_DEFAULT_REGION"] = creds_json['AWS_DEFAULT_REGION']
                log(f"Set AWS_DEFAULT_REGION={creds_json['AWS_DEFAULT_REGION']}")
            
            log("Successfully set AWS credentials from credentials file")
        
        return True
        
    except Exception as e:
        error(f"Failed to load credentials for {cloud}: {e}")
        return False


# Also include the create_cloud_event function since it's related to testing
def create_cloud_event(cloud, object_path, bucket_name):
    """
    Create a minimal cloud event for testing based on the cloud provider.
    
    Args:
        cloud (str): Cloud provider ("Amazon", "Azure", or "Google")
        object_path (str): Path to the object in the bucket
        bucket_name (str): Name of the bucket/container
        
    Returns:
        dict or object: A minimal cloud event in the format expected by the cloud function
    """
    if cloud == "Amazon":
        return {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": bucket_name},
                        "object": {"key": object_path}
                    }
                }
            ]
        }
    elif cloud == "Azure":
        class MockAzureEvent:
            def __init__(self, url):
                self._json_data = {"url": url}
                self.event_type = "Microsoft.Storage.BlobCreated"
            def get_json(self):
                return self._json_data
        return MockAzureEvent(f"https://terraformsaccb42.blob.core.windows.net/{bucket_name}/{object_path}")
    elif cloud == "Google":
        class MockCloudEvent:
            def __init__(self, data, attributes=None):
                self.data = data
                self.attributes = attributes or {}
            def __repr__(self):
                return f"MockCloudEvent(data={self.data}, attributes={self.attributes})"
        return MockCloudEvent(data={'name': object_path})
    else:
        raise ValueError(f"Unsupported cloud provider: {cloud}")
