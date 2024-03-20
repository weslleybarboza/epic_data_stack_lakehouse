"""
An example DAG that uses Cosmos to render a dbt project.
"""

import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProjectConfig, ProfileConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_ROOT_PATH = "/opt/airflow/dbts"

profile_config = ProfileConfig(
    profile_name="trino",
    target_name="dev",
    profiles_yml_filepath= DBT_ROOT_PATH + '/profiles.yml'
    )

# [START local_example]
basic_cosmos_dag = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(
        DBT_ROOT_PATH,
        models_relative_path=DBT_ROOT_PATH + '/dbt-model'
    ),
    profile_config=profile_config,
    operator_args={
        "install_deps": True,  # install any necessary dependencies before running any dbt command
        "full_refresh": True,  # used only in dbt commands that support this flag
    },
    # normal dag parameters
    schedule_interval="@daily",
    start_date=datetime(2024, 3, 20),
    catchup=False,
    dag_id="basic_cosmos_dag",
    default_args={"retries": 0},
)
# [END local_example]