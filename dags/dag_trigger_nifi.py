from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import requests
import logging
import time

##############################################################################################
# DAG to monitor data sources and ingest into landing zone - runs every 5 minutes
##############################################################################################

# Define default arguments for the DAG
# 'depends_on_past': False --> task's execution should not be influenced by its past runs
default_args = {
    'owner': 'airflow',
    'depends_on_past': False, 
    'start_date': datetime(2024, 3, 15)
}

dag = DAG(
    dag_id='monitor_data_sources',
    default_args=default_args,
    description='Monitor data sources and ingest into landing zone',
    schedule_interval=timedelta(minutes=5),
    catchup = False
)

##############################################################################################
# Custom Functions
##############################################################################################

# Function to determine the next task based on whether files are found
def check_files_found():
    """
    Checks whether files are found in the data sources.

    This function returns the task ID of the next task based on whether files are found.
    """
    if True:
        return 'activate_nifi_processor_group_task'
    else:
        return 'log_no_files_task'

# Function to log a message if no files are found
def log_no_files_found():
    """
    Logs a message if no files are found in the data sources.
    """
    logging.info("No files found in the data sources")

# NiFi Configs
nifi_host = 'nifi'
nifi_port = '8443'
process_group_id = 'f5f10970-0e9b-30c3-e779-3f56995cc788' # this is specific for pscore/ascll directory
process_group_headers = {"Content-Type": "*/*"}
process_group_url = f"http://{nifi_host}:{nifi_port}/nifi-api/process-groups/{process_group_id}/processors"
processor_header = {"Content-Type": "application/json"}

def get_process_ids_and_current_version():

    # try except clause to enter processor group and read the processors ids
    try:
        process_group_response = requests.get(process_group_url, headers=process_group_headers)
        # If 2xx response continues loop, if not exception arises
        process_group_response.raise_for_status() 
        logging.info("Processor ids and version read successfully")

        # Loop through all the processors in the process group to get the processor ids and current revsision versions
        processor_info_list = [(processor['id'], processor['revision']['version']) 
                               for processor in process_group_response.json()["processors"]]
        
        return processor_info_list

    except requests.exceptions.RequestException as e:
        error_message = "Failed to read processor ids and versions"
        logging.error(error_message)
        raise Exception(e)

# Function to activate the NiFi processors in (specific) process group 
def activate_nifi_processor_group():
    """
    Activates the NiFi processor group - In this POC taking into account pscore/ascll directory.

    This function sends a GET request to the NiFi API to get the ids & current version of the 
    processors in the process group. Once found, loop through the ids to start the processors.
    """

    processor_info_list = get_process_ids_and_current_version()

    # Loop through processors in process group to START them up
    for processor_id, processor_version in processor_info_list:

        # Payload (body) for each processor
        payload_running = {"revision": {"clientId": "1","version": processor_version},"state": "RUNNING"}
        processor_url = f"http://{nifi_host}:{nifi_port}/nifi-api/processors/{processor_id}/run-status"
    
        try:
            # Start the NiFi processor
            response = requests.put(processor_url, json=payload_running, headers=processor_header) 
            response.raise_for_status() 
            logging.info(f"NiFi processor id {processor_id} activated successfully")

        except requests.exceptions.RequestException as e:
            error_message = f"Failed to activate NiFi processor id {processor_id}"
            logging.error(error_message)
            raise Exception(e)

# Function to stop the NiFi processors in (specific) process group 
def stop_nifi_processor_group():

    """
    Stops the NiFi processor group - In this POC taking into account pscore/ascll directory.

    This function sends a GET request to the NiFi API to get the ids & current version of the 
    processors in the process group. Once found, loop through the ids to stop the processors.
    """

    # Sleep 30 seconds to wait for nifi process to run - FOR DEMO PURPOSE
    time.sleep(30)

    processor_info_list = get_process_ids_and_current_version()

    # Loop through processors in process group to START them up
    for processor_id, processor_version in processor_info_list:

        # Payload (body) for each processor
        payload_stopped = {"revision": {"clientId": "1","version": processor_version},"state": "STOPPED"}
        processor_url = f"http://{nifi_host}:{nifi_port}/nifi-api/processors/{processor_id}/run-status"

        try:
            # Stop the NiFi processor regardless of whether there was an error or not
            response = requests.put(processor_url, json=payload_stopped, headers=processor_header) 
            response.raise_for_status() 
            logging.info(f"NiFi processor id {processor_id} stopped successfully")
        except requests.exceptions.RequestException as e:
            error_message = f"Failed to stop NiFi processor id {processor_id}"
            logging.error(error_message)
            raise Exception(e)
  
##############################################################################################
# First portion of the DAG is to define the FileSensor 
##############################################################################################

# FileSensor monitors the landing zone directory recursively checking for files
landing_zone_dir = '/opt/airflow/sources'
file_sensor = FileSensor(
    task_id='monitor_data_sources_task',
    poke_interval=10,  # Check for files every 10 seconds
    timeout=30,  # Timeout after 30 seconds
    filepath=landing_zone_dir,
    fs_conn_id='filesystem_connection',
    recursive=True,  # Enable recursive monitoring
    soft_fail=True,  # Don't raise error if files are not found
    dag=dag,
)

# Log a message if no files are found
# Trigger rule - Will run if upstream (branch) has not failed i.e. either skipped or succeeded
log_no_files_task = PythonOperator(
    task_id='log_no_files_task',
    python_callable=log_no_files_found,
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag,
)

##############################################################################################
# Second portion of the DAG is to determine which task to execute next
##############################################################################################

# BranchPythonOperator to either activate the nifi processor or log that no files were found
branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=check_files_found,
    dag=dag,
)

##############################################################################################
# Third portion of the DAG is to activate the Nifi processors
##############################################################################################

# Once files are found in landing zone, PythonOperator runs activate_nifi_processor_group
activate_nifi_processor_group_task = PythonOperator(
        task_id='activate_nifi_processor_group_task',
        python_callable=activate_nifi_processor_group,
        provide_context=True,
        dag=dag,
    )

##############################################################################################
# Fourth portion of the DAG is to stop the Nifi processors
##############################################################################################

# Once files are found in landing zone, PythonOperator runs activate_nifi_processor_group
stop_nifi_processor_group_task = PythonOperator(
        task_id='stop_nifi_processor_group_task',
        python_callable=stop_nifi_processor_group,
        provide_context=True,
        dag=dag,
    )

##############################################################################################
# Set task dependencies
##############################################################################################

# Define DAG structure
file_sensor >> branch_task >> [activate_nifi_processor_group_task, log_no_files_task]
activate_nifi_processor_group_task >> stop_nifi_processor_group_task