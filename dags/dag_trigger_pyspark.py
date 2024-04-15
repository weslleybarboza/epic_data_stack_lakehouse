from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from docker.types import Mount
from docker.types import Resources
from datetime import datetime

##############################################################################################
# DAG to run pyspark script which gets data from landing zone to bronze layer of data lake
##############################################################################################

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 18)
}

# Define the DAG
dag = DAG(
    dag_id = 'pyspark_docker_operator',
    description='DAG with DockerOperator and spark-submit commands',
    schedule_interval=None,
    start_date=datetime(2024, 3, 22),
    catchup=False
)

##############################################################################################
# Custom Funcs
##############################################################################################

def read_spark_config(config_file):
    """
    Read Spark configuration from the given file and generate --conf arguments for spark-submit.

    Args:
        config_file (str): Path to the spark-defaults.conf file.

    Returns:
        list: List of --conf arguments.
    """
    conf_args = []

    with open(config_file, 'r') as file:
        for line in file:
            line = line.strip()
            # Ignore comments and empty lines
            if not line or line.startswith('#'):
                continue
            # Split each line into key and value
            key, value = line.split(' ', 1)
            # Generate --conf argument
            conf_args.append(f"--conf {key.strip()}={value.strip()}")

    return ' '.join(conf_args) 

# Pyspark configs
image = 'weslleybarboza/spark-3.4:latest'
network_name = 'lakehouse'
mount_script_source = '/c/Projects/Development/epic_data_stack_lakehouse/notebook/'
mount_script_target = '/opt/spark/notebook'
mount_configs_source = '/c/Projects/Development/epic_data_stack_lakehouse/docker/spark/spark-defaults-iceberg.conf'
mount_configs_target = '/opt/spark/conf/spark-defaults.conf'

pyspark_script_name = 'sgw_raw_to_lakehouse_v5.py'
pyspark_script_dir = f'{mount_script_target}/{pyspark_script_name}'

# Define environment variables
environment_variables = {
    'AWS_ACCESS_KEY_ID': 'minio',
    'AWS_SECRET_ACCESS_KEY': 'minio123',
    'AWS_REGION': 'us-east-1',
    'AWS_DEFAULT_REGION': 'us-east-1',
    'S3_ENDPOINT': 'http://minio:9000',
    'S3_PATH_STYLE_ACCESS': 'true',
}

# # Define resource limits
# resource_limits = {
#     'mem_limit': '8g',
#     'mem_reservation': '512m',
#     'cpus': '4',
#     'cpuset': '4',
# }

##############################################################################################
# Function to generate string of configs to be used in spark-submit command  
##############################################################################################

spark_configs = read_spark_config(mount_configs_target)

##############################################################################################
# DockerOperator uses the spark image to create a custom container and execute spark script 
##############################################################################################

spark_submit_command = f'--master local[*] --deploy-mode client {spark_configs} {pyspark_script_dir}'

run_pyspark_script = DockerOperator(
        task_id = 'run_pyspark_script',
        image = image,
        command = spark_submit_command,
        container_name = 'pyspark_docker_operator',
        environment = environment_variables, # Env vars t connect to s3
        # docker_url='unix://var/run/docker.sock',
        network_mode = network_name,
        mount_tmp_dir = False, # Disable mounting temporary directory
        auto_remove = 'force', # remove docker container after executing command
        entrypoint = "spark-submit", # Changing entrypoint to execute spark-submit command 
        mounts=[
            Mount(
                source=mount_script_source, 
                target=mount_script_target, 
                type='bind'), # mount for the pyspark script
            Mount(
                source=mount_configs_source, 
                target=mount_configs_target, 
                type='bind') # mount for the configs
            ],
        mem_limit = '8g', # mem_reservation unsure how to setup
        cpus = 4, # cpuset unsure how to setup
        dag = dag,
    )

##############################################################################################
# Set task dependencies
##############################################################################################

# Define DAG structure

run_pyspark_script