from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

##############################################################################################
# DAG to run Jupyter notebook which gets data from landing zone to bronze layer of data lake
##############################################################################################


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 18)
}

dag = DAG(
    dag_id='run_jupyter_notebook',
    default_args=default_args,
    description='A simple DAG to run a Jupyter notebook',
    schedule_interval=None
)

# Scripts directory for jupyter notebook 
raw_to_lakehouse_dir = '/opt/airflow/notebook/sgw_raw_to_lakehouse_v5.py'
spark_conf_file = '/opt/airflow/spark/conf/spark-defaults.conf'

# Read the contents of your Spark configuration file
with open(spark_conf_file, 'r') as file:
    spark_conf = file.read()

# Convert the configuration to a dictionary
spark_conf_dict = {}
for line in spark_conf.split('\n'):
    if line.strip() and not line.startswith('#'):
        key, value = line.split(' ', 1)
        spark_conf_dict[key.strip()] = value.strip()

# Instantiate a SparkSubmitOperator to run your PySpark script
submit_spark_job = SparkSubmitOperator(
    task_id='submit_spark_job',
    conn_id='spark_default',  # Connection ID configured in Airflow for Spark - DEFAULT
    application=raw_to_lakehouse_dir,  # Path to your PySpark script
    conf=spark_conf_dict,
    dag=dag
)

##############################################################################################
# Set task dependencies
##############################################################################################

# Define DAG structure
submit_spark_job
