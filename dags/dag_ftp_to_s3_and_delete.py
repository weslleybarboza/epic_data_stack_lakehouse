from airflow import DAG
from airflow.providers.amazon.aws.transfers.ftp_to_s3 import FTPToS3Operator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.ftp.hooks.ftp import FTPHook
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 17)
}

dag = DAG(
    'ftp_to_s3_and_delete_from_ftp',
    default_args=default_args,
    description='Transfer files from FTP to S3 and delete from FTP',
    schedule_interval='@daily'
)

##############################################################
# Functions
##############################################################

# Definining FTP directory & FTPHook to interact with FTP directory
ftp_folder = '/pscore/ascll/'
ftp_hook = FTPHook(ftp_conn_id='ftp_connection')

# Function to return the list of files in FTP directory
def list_files_in_ftp():
    return ftp_hook.list_directory(ftp_folder) # os.listdir(ftp_folder)

# Function to use the list of files in FTP directory from list_files_in_ftp function
# Use FTPHook to interact with FTP directory - in this case delete files
def remove_files_from_ftp(**kwargs):
    ftp_files = kwargs['ti'].xcom_pull(task_ids='list_files_in_ftp')
    ftp_hook = FTPHook(ftp_conn_id='ftp_connection')
    try:
        for file_name in ftp_files:
            file_path = ftp_folder + file_name
            ftp_hook.delete_file(file_path)
            logger.info(f"Deleted file {file_name} from FTP server.")
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)

##############################################################
# Tasks
##############################################################

# Implementing list_files_in_ftp function
list_files_task = PythonOperator(
    task_id='list_files_in_ftp',
    python_callable=list_files_in_ftp,
    dag=dag,
)

# Transfering files from FTP to S3
ftp_to_s3_task = FTPToS3Operator(
    task_id='transfer_ftp_to_s3',
    ftp_conn_id='ftp_connection',
    aws_conn_id='s3_connection',
    ftp_path='/pscore/ascll/',
    ftp_filenames=list_files_task.output,
    s3_bucket='ftptestingairflow',
    s3_key='',
    replace=True, # If file exists in S3, overwrite
    dag=dag,
)

# Implementing remove_files_from_ftp function
remove_files_task = PythonOperator(
    task_id='remove_files_from_ftp',
    python_callable=remove_files_from_ftp,
    provide_context=True,
    dag=dag,
)

##############################################################
# Task Dependencies
##############################################################
list_files_task >> ftp_to_s3_task >> remove_files_task
