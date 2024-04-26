import io
import gzip
from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
import logging
import boto3
from botocore.client import Config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 17),
}

dag = DAG(
    's3_to_s3_transfer_with_unzip',
    default_args=default_args,
    description='Transfer files from S3 to same S3 bucket with unzipping',
    schedule_interval='@daily',
)

##############################################################
# S3 configs
##############################################################

# Listing objects
# s3 connection using boto3 with env variables
endpoint_url = "http://minio:9000"
access_key = "0Pvn4so7Xj326eMHh495"
secret_key = "DBSxHV5ztFTxlMXOwDcnmuJdIGUgh3XvlXFLYo2a"
bucket_name = "ftptestingairflow"

s3 = boto3.client('s3',
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                config=Config(signature_version="s3v4"),
                verify=False)

# Create an S3Hook
s3_hook = S3Hook(aws_conn_id='s3_connection')

##############################################################
# Functions
##############################################################

def gunzip_files_in_s3_bucket(bucket_name, prefix=''):

    # Get s3 objects
    objects = s3.list_objects_v2(Bucket=bucket_name)

    # Extract keys (file paths) from the response
    keys = [obj['Key'] for obj in objects.get('Contents', [])]

    for key in keys:
        if key.endswith('.gz'):

            # Download gzip file content
            gzip_object = s3.get_object(Bucket=bucket_name, Key=key)
            gzip_content = gzip_object['Body'].read()

            # Unzip the file content
            with gzip.open(io.BytesIO(gzip_content), 'rb') as gz_file:
                unzipped_content = gz_file.read()

            # Define the new key for the unzipped file (remove .gz extension)
            unzipped_key = key[:-3]

            # Upload the unzipped content back to the same S3 bucket
            s3.put_object(Bucket=bucket_name, Key=unzipped_key, Body=unzipped_content)

            print(f"Unzipped {key} to {unzipped_key} in {bucket_name}")

##############################################################
# Tasks
##############################################################

# Task to unzip files with .gz extension
gunzip_task  = PythonOperator(
    task_id='unzip_files_in_s3',
    python_callable=gunzip_files_in_s3_bucket,
    op_kwargs={'bucket_name': bucket_name, 'prefix': ''},
    dag=dag,
)

##############################################################
# Task Dependencies
##############################################################
gunzip_task