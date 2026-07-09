# Upload all files in tmp_output_dir to cloud storage
def process_decoded_data(cloud, storage_client, bucket_output, tmp_output_dir, logger, routing_rule=None, mirror_to_default=False):
    from .utils import upload_files_to_cloud

    result = upload_files_to_cloud(cloud, storage_client, bucket_output, tmp_output_dir, routing_rule, mirror_to_default)

    return result