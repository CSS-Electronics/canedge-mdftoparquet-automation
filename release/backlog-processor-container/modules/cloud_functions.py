import json
import os
import logging

# Note: load_credentials and load_creds_file_into_env functions
# have been moved to utils_testing.py
        
def get_log_file_object_paths(cloud, event, logger):
    """
    Extract a list of object paths from the event.
    Handles both single-record events using cloud-specific logic and list-type events using backlog.json format.
    
    Args:
        cloud (str): Cloud provider ("Amazon", "Google", or "Azure")
        event (list, dict or object): The event data structure
        logger: Logger object for logging messages
        
    Returns:
        list: List of Path objects representing object paths
    """
    from pathlib import Path
    from urllib.parse import unquote_plus
    
    # Valid log file extensions
    valid_extensions = [".MF4", ".MFC", ".MFE", ".MFM"]
    
    # Helper function to check if a file has valid extension
    def has_valid_extension(filename):
        return any(filename.upper().endswith(ext) for ext in valid_extensions)
    
    from urllib.parse import urlparse
    
    log_file_object_paths = []
    
    # If event is a list, assume it follows the backlog.json structure regardless of cloud type
    # Otherwise handle events in cloud specific manner
    try:
        if isinstance(event, list):
            logger.info(f"Processing list-type event with {len(event)} items")
            for item in event:
                if isinstance(item, str):
                    if has_valid_extension(item):
                        # logger.info(f"Adding object path from list: {item}")
                        log_file_object_paths.append(Path(item))
        else:
            if cloud == "Amazon":
                if "Records" in event:
                    for record in event["Records"]:
                        if "s3" in record and "object" in record["s3"] and "key" in record["s3"]["object"]:
                            object_key = unquote_plus(record["s3"]["object"]["key"])
                            if has_valid_extension(object_key):
                                log_file_object_paths.append(Path(object_key))
                            
            elif cloud == "Azure":
                data = event.get_json()
                url = data.get('url')
                # Extract blob path directly from URL
                parsed_url = urlparse(url)
                path_parts = parsed_url.path.split('/')
                object_key = '/'.join(path_parts[2:]) if len(path_parts) >= 3 else None
                
                logger.info(f"Extracted object key: {object_key}")
                if object_key and has_valid_extension(object_key):
                    log_file_object_paths.append(Path(object_key))
                    
            elif cloud == "Google":
                if hasattr(event, 'data') and 'name' in event.data:
                    file_name = event.data['name']
                    if has_valid_extension(file_name):
                        log_file_object_paths.append(Path(file_name))
            else:
                logger.error(f"Unsupported cloud provider: {cloud}")
        
        # logger.info(f"Log file object paths: {log_file_object_paths}")
        return log_file_object_paths
        
    except Exception as e:
        logger.error(f"Failed to extract object paths from event: {e}")
        return []
        

def normalize_object_path(path):
    path_str = str(path) if not isinstance(path, str) else path
    return path_str.replace('\\', '/')

def download_object(cloud, client, bucket, object_path, local_path, logger, supress=False):
    """
    Download an object from a cloud storage bucket to a local file.
    
    Args:
        cloud (str): Cloud provider ("Amazon", "Google", "Azure", or "Local")
        client: Cloud storage client
        bucket (str): Bucket or container name
        object_path (str): Path to the object in the bucket
        local_path (str): Local path to save the object
        logger: Logger object for logging messages
        
    Returns:
        bool: True if download was successful, False otherwise
    """
    import os
    from pathlib import Path
    
    # Normalize object path for cloud providers (but not for Local)
    if cloud != "Local":
        object_path = normalize_object_path(object_path)
    
    if cloud == "Amazon":
        try:
            # Download the object from S3
            client.download_file(bucket, object_path, local_path)
            if logger and supress == False:
                logger.info(f"Successfully downloaded {object_path} from {bucket} to {local_path}")
            return True
        except Exception as e:
            if logger:
                logger.error(f"Failed to download {object_path} from {bucket}: {e}")
            return False
    elif cloud == "Google":
        try:
            # Get the bucket
            gcp_bucket = client.bucket(bucket)
            # Get the blob
            blob = gcp_bucket.blob(object_path)
            
            # Make sure the directory exists
            os.makedirs(os.path.dirname(str(local_path)), exist_ok=True)
            
            # Download the blob
            blob.download_to_filename(str(local_path))
            
            if supress == False:
                logger.info(f"Downloaded {bucket}/{object_path} to {local_path}")
            return True
        except Exception as e:
            logger.info(f"Failed to download {bucket}/{object_path}")
            return False
    elif cloud == "Azure":
        try:
            # Get the container client
            container_client = client.get_container_client(bucket)
            # Get the blob client
            blob_client = container_client.get_blob_client(object_path)
            
            # Download the blob
            with open(str(local_path), "wb") as file:
                download_stream = blob_client.download_blob()
                file.write(download_stream.readall())
            
            if supress == False:
                logger.info(f"Downloaded {bucket}/{object_path} to {local_path}")
            return True
        except Exception as e:
            logger.info(f"Failed to download {bucket}/{object_path}")
            return False
    elif cloud == "Local":
        try:
            import shutil
            
            # For local storage, simply copy the file
            source_path = os.path.join(bucket, object_path)
            
            # Ensure destination directory exists
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Copy the file
            shutil.copy2(source_path, local_path)
            
            if logger and supress == False:
                logger.info(f"Copied {object_path} from Local storage: {bucket} to {local_path}")
            return True
        except Exception as e:
            if logger:
                logger.error(f"Failed to copy {object_path} from local storage {bucket}: {e}")
            return False
    else:
        logger.error(f"Unsupported cloud provider: {cloud}")
        return False


def upload_object(cloud, client, bucket, object_path, local_path, logger):
    """
    Upload a local file to a cloud storage bucket.
    
    Args:
        cloud (str): Cloud provider ("Amazon", "Google", "Azure", or "Local")
        client: Cloud storage client
        bucket (str): Bucket or container name
        object_path (str): Path to store the object in the bucket
        local_path (str): Local path of the file to upload
        logger: Logger object for logging messages
        
    Returns:
        bool: True if upload was successful, False otherwise
    """
    # Normalize object path for cloud providers (but not for Local)
    if cloud != "Local":
        object_path = normalize_object_path(object_path)
    
    if cloud == "Amazon":
        try:
            client.upload_file(str(local_path), bucket, object_path)
            logger.info(f"Uploaded object to {bucket}/{object_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload object to {bucket}/{object_path}: {e}")
            return False
    elif cloud == "Google":
        try:
            gcp_bucket = client.bucket(bucket)
            blob = gcp_bucket.blob(object_path)
            blob.upload_from_filename(str(local_path))
            logger.info(f"Uploaded object to {bucket}/{object_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload object to {bucket}/{object_path}: {e}")
            return False
    elif cloud == "Azure":
        try:
            container_client = client.get_container_client(bucket)
            blob_client = container_client.get_blob_client(object_path)
            with open(str(local_path), "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            logger.info(f"Uploaded object to {bucket}/{object_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload object to {bucket}/{object_path}: {e}")
            return False
    elif cloud == "Local":
        try:
            import shutil
            import os
            from pathlib import Path
            
            # For local storage, simply copy the file
            dest_path = os.path.join(bucket, object_path)
            
            # Ensure destination directory exists
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            
            # Copy the file
            shutil.copy2(str(local_path), dest_path)
            
            if logger:
                logger.info(f"Uploaded {local_path} to Local storage: {dest_path}")
            return True
        except Exception as e:
            if logger:
                logger.error(f"Failed to upload {local_path} to local storage {bucket}/{object_path}: {e}")
            return False
    else:
        logger.error(f"Unsupported cloud provider: {cloud}")
        return False


def list_objects(cloud, client, bucket, logger, prefix="", supress=False):
    """
    List objects in a cloud storage bucket.
    
    Args:
        cloud (str): Cloud provider ("Amazon", "Google", "Azure", or "Local")
        client: Cloud storage client
        bucket (str): Bucket or container name
        logger: Logger object for logging messages
        prefix (str): Object prefix to filter results
        
    Returns:
        dict: Standardized response with 'objects' key containing a list of object dictionaries
              with 'name' and other metadata, or an empty list if no objects or error
    """
    if cloud == "Amazon":
        try:
            response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            if supress == False:
                logger.info(f"Listed objects in {bucket} with prefix {prefix}")
            
            # Convert AWS-specific response to standardized format
            result = []
            if "Contents" in response:
                for item in response["Contents"]:
                    result.append({"name": item["Key"], "size": item["Size"], "last_modified": item["LastModified"]})
            
            return {"objects": result}
        except Exception as e:
            logger.error(f"Failed to list objects in {bucket} with prefix {prefix}: {e}")
            return {"objects": []}
    elif cloud == "Google":
        try:
            gcp_bucket = client.bucket(bucket)
            blobs = gcp_bucket.list_blobs(prefix=prefix)
            result = []
            for blob in blobs:
                result.append({
                    "name": blob.name,
                    "size": blob.size,
                    "last_modified": blob.updated
                })
                
            if supress == False:
                logger.info(f"Listed objects in GCP bucket {bucket} with prefix {prefix}")
            return {"objects": result}
        except Exception as e:
            logger.error(f"Failed to list objects in GCP bucket {bucket} with prefix {prefix}: {e}")
            return {"objects": []}
    elif cloud == "Azure":
        try:
            container_client = client.get_container_client(bucket)
            result = []
            blobs = container_client.list_blobs(name_starts_with=prefix)
            for blob in blobs:
                result.append({
                    "name": blob.name,
                    "size": blob.size,
                    "last_modified": blob.last_modified
                })
            
            if supress == False:
                logger.info(f"Listed objects in Azure container {bucket} with prefix {prefix}")
            return {"objects": result}
        except Exception as e:
            logger.error(f"Failed to list objects in Azure container {bucket} with prefix {prefix}: {e}")
            return {"objects": []}
    elif cloud == "Local":
        try:
            from pathlib import Path
            import os
            
            response = {"objects": []}
            bucket_path = Path(bucket)
            
            # For Local storage, handle prefix filtering differently
            # We need to walk the directory and filter files by prefix at the filename level
            for root, _, files in os.walk(bucket_path):
                for file in files:
                    file_path = Path(root) / file
                    # Convert to relative path from bucket
                    rel_path = str(file_path.relative_to(bucket_path)).replace('\\', '/')
                    
                    # If prefix is specified, check if the filename (not just path) starts with the prefix
                    if not prefix or rel_path.lower().startswith(prefix.lower()):
                        response["objects"].append({
                            "name": rel_path,
                            "size": file_path.stat().st_size,
                            "last_modified": file_path.stat().st_mtime
                        })
                        
            if logger and supress == False:
                logger.info(f'Listed {len(response["objects"])} objects in {bucket} with prefix {prefix}')
            return response
            
        except Exception as e:
            logger.error(f"Failed to list objects in {bucket} with prefix {prefix}: {e}")
            return {"objects": []}
    else:
        logger.error(f"Unsupported cloud provider: {cloud}")
        return {"objects": []}


def list_objects_with_pagination(cloud, client, bucket, logger, prefix="", supress=False):
    """
    List objects in a cloud storage bucket with pagination support for >1000 objects.
    
    Args:
        cloud (str): Cloud provider ("Amazon", "Google", "Azure", or "Local")
        client: Cloud storage client
        bucket (str): Bucket or container name
        logger: Logger object for logging messages
        prefix (str): Object prefix to filter results
        
    Returns:
        dict: Standardized response with 'objects' key containing a list of all object dictionaries
              with 'name' and other metadata, or an empty list if no objects or error
    """
    all_objects = []
    
    if cloud == "Amazon":
        try:
            # Initialize pagination variables
            continuation_token = None
            is_truncated = True
            
            while is_truncated:
                # Set up the list_objects_v2 parameters
                params = {"Bucket": bucket, "Prefix": prefix}
                if continuation_token:
                    params["ContinuationToken"] = continuation_token
                
                # Call the API
                response = client.list_objects_v2(**params)
                
                # Process the results
                if "Contents" in response:
                    for item in response["Contents"]:
                        all_objects.append({
                            "name": item["Key"], 
                            "size": item["Size"], 
                            "last_modified": item["LastModified"]
                        })
                
                # Check if there are more results
                is_truncated = response.get("IsTruncated", False)
                continuation_token = response.get("NextContinuationToken", None)
            
            return {"objects": all_objects}
            
        except Exception as e:
            logger.error(f"Failed to list objects in {bucket} with prefix {prefix}: {e}")
            return {"objects": []}
            
    elif cloud == "Google":
        try:
            # Initialize pagination variables
            page_token = None
            gcp_bucket = client.bucket(bucket)
            
            # Loop until all pages are processed
            while True:
                # Get a page of results
                blobs = gcp_bucket.list_blobs(prefix=prefix, page_token=page_token)
                
                # Process the current page
                current_batch = list(blobs)
                for blob in current_batch:
                    all_objects.append({
                        "name": blob.name,
                        "size": blob.size,
                        "last_modified": blob.updated
                    })
                
                # Check if there are more pages
                page_token = blobs.next_page_token
                if not page_token:
                    break
            
            return {"objects": all_objects}
            
        except Exception as e:
            logger.error(f"Failed to list objects in GCP bucket {bucket} with prefix {prefix}: {e}")
            return {"objects": []}
            
    elif cloud == "Azure":
        try:
            # Initialize the container client and pagination variables
            container_client = client.get_container_client(bucket)
            continuation_token = None
            
            # Loop until all pages are processed
            while True:
                # Get a page of results
                blobs_page = container_client.list_blobs(
                    name_starts_with=prefix,
                    results_per_page=1000,
                    marker=continuation_token
                )
                
                # Convert to a list so we can check its length and get the last item
                current_batch = list(blobs_page)
                
                # Process the current page
                for blob in current_batch:
                    all_objects.append({
                        "name": blob.name,
                        "size": blob.size,
                        "last_modified": blob.last_modified
                    })
                
                # Check if we've reached the end
                if not current_batch or len(current_batch) < 1000:
                    break
                    
                # Update the continuation token for the next page
                continuation_token = current_batch[-1].name
            
            return {"objects": all_objects}
            
        except Exception as e:
            logger.error(f"Failed to list objects in Azure container {bucket} with prefix {prefix}: {e}")
            return {"objects": []}
    elif cloud == "Local":
        try:
            # For Local storage, just use the regular list_objects function
            # as pagination isn't typically needed for local filesystem operations
            return list_objects(cloud, client, bucket, logger, prefix, supress)
        except Exception as e:
            logger.error(f"Failed to list objects in {bucket} with prefix {prefix}: {e}")
            return {"objects": []}
    else:
        logger.error(f"Unsupported cloud provider: {cloud}")
        return {"objects": []}


def publish_notification(cloud, client, subject, message, logger):
    """
    Publish a notification to a cloud messaging service.
    
    Args:
        cloud (str): Cloud provider ("Amazon", "Google", "Azure", or "Local")
        client: Cloud notification client
        subject (str): Notification subject
        message (str): Notification message body
        message_attributes (dict): Additional message attributes
        logger: Logger object for logging messages
        
    Returns:
        bool: True if notification was published successfully, False otherwise
    """
    import os
    if cloud == "Amazon":
        if notification_client == None or notification_client == False:
            logger.info(f"- No message client available")
            return False
        
        target = os.environ.get("SNS_ARN", "NONE")
        message_attributes = {'DeduplicationId': {'DataType': 'String','StringValue': subject.replace(' ', '_').replace("|","")}} 
        try:
            response = client.publish(
                TopicArn=target,
                Subject=subject,
                Message=message,
                MessageAttributes=message_attributes
            )
         
            logger.info(f"Published message with subject '{subject}' to SNS topic: {target}")
            return True
        except Exception as e:
            logger.error(f"Error publishing to SNS: {e}")
            return False
    elif cloud == "Google" and notification_client:
        # Below will trigger a GCP Metric --> Alert --> Notification based on the payload containing 'NEW EVENT'
        logger.info(f"NEW EVENT: {message}")
    elif cloud == "Azure" and notification_client:
        # Add NEW EVENT log pattern for Azure Monitor to detect
        logger.info(f"NEW EVENT: {message}")
        return True
    elif cloud == "Local":
        # For Local, just log the message
        logger.info(f"LOCAL NOTIFICATION - Subject: {subject}, Message: {message}")
        return True
    else:
        logger.error(f"Unsupported cloud provider: {cloud}")
        return False
