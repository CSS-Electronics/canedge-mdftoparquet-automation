# Upload all files in tmp_output_dir to cloud storage
def process_decoded_data(cloud, storage_client, bucket_output, tmp_output_dir, logger):   
    from .utils import upload_files_to_cloud 
    
    result = upload_files_to_cloud(cloud, storage_client, bucket_output, tmp_output_dir) 
    
    return result