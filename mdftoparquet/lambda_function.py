import boto3
import os
from modules.mdf_to_parquet import mdf_to_parquet 

# Cloud provider configuration
cloud = "Amazon"
storage_client = boto3.client("s3")
notification_client = boto3.client("sns")

def lambda_handler(event, context):   
    bucket_input = os.environ.get('INPUT_BUCKET')
    bucket_output = bucket_input + "-parquet"
    
    return mdf_to_parquet(cloud, storage_client, notification_client, event, bucket_input, bucket_output)