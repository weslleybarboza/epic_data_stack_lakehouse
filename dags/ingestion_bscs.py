from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define default_args dictionary to specify the default parameters of the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

file_path   = '/opt/notebook/src/'
file_source = 'bscs'
file_name   = 'bscs_contract_all.py'
file_description = 'Refined table'

dag_tags = ['bronze', 'refined', file_source]

# Define the execution date for today
today = datetime.combine(datetime.today(), datetime.min.time())

# Instantiate a DAG
dag = DAG(
    f'{file_name}',
    default_args=default_args,
    description=file_description,
    schedule_interval='@once',  # Run the DAG only once
    start_date=today,  # Start the DAG on today's date
    tags=dag_tags
)

# Define the path to your Python script inside the container
script_path = f'{file_path}/{file_source}/{file_name}'

# Define the spark-submit command
spark_submit_cmd = f'docker exec spark-icebert spark-submit --deploy-mode client {script_path}'

# Create a BashOperator to submit the Spark job
submit_spark_job = BashOperator(
    task_id=f'submit_spark_job',
    bash_command=spark_submit_cmd,
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()
