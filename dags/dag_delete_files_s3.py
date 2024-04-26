from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import boto3
from botocore.client import Config

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 17),
}

dag = DAG(
    'delete_cdr_files_from_s3',
    default_args=default_args,
    description='Delete .cdr files from an S3 bucket',
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
# Functions - Delete files ending with .cdr
# https://airflow.apache.org/docs/apache-airflow/1.10.13/_api/airflow/contrib/operators/s3_delete_objects_operator/index.html
##############################################################


# Define the keys (files) to delete
# keys_to_delete = List objects in s3 bucket and filter for those that end with '.cdr' 
def list_cdr_files():
    response = s3.list_objects_v2(Bucket=bucket_name)
    files = response.get('Contents', [])  # Get the list of files or an empty list if 'Contents' key is not present
    cdr_files = [file['Key'] for file in files if file['Key'].endswith('.cdr')]
    return cdr_files

cdr_files_to_delete = list_cdr_files()

# Define the S3DeleteObjectsOperator
delete_files_task = S3DeleteObjectsOperator(
    task_id='delete_files_task',
    bucket='ftptestingairflow',
    keys=cdr_files_to_delete,
    aws_conn_id='s3_connection',
    dag=dag,
)

##############################################################
# Task Dependencies
##############################################################
delete_files_task