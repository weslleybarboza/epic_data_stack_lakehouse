from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_submit_dag_docker',
    default_args=default_args,
    description='A DAG to submit a Spark job',
    schedule_interval=timedelta(days=1),
)

spark_submit_cmd = """
ls -lh
"""

# spark_submit_cmd = """
# spark-submit \
#     --deploy-mode client \
#     /opt/notebook/src/bscs/bscs_contract_all.py
# """

print('Command: ',spark_submit_cmd)

submit_spark_job = DockerOperator(
    task_id='submit_spark_job',
    image='weslleybarboza/spark-3.4:latest',    
    container_name='spark-iceberg',  # Use existing container    
    command=spark_submit_cmd,
    network_mode='bridge',
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()
