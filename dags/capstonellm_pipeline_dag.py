import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator

IMAGE = "capstonellm:latest"
TAG = "python-polars"

AWS_ENV = {
    key: value
    for key, value in (
        ("AWS_ACCESS_KEY_ID", os.environ.get("AWS_ACCESS_KEY_ID")),
        ("AWS_SECRET_ACCESS_KEY", os.environ.get("AWS_SECRET_ACCESS_KEY")),
        ("AWS_SESSION_TOKEN", os.environ.get("AWS_SESSION_TOKEN")),
        ("AWS_DEFAULT_REGION", os.environ.get("AWS_DEFAULT_REGION", "eu-west-1")),
    )
    if value
}

default_args = {
    "owner": "airflow",
    "description": "Ingest StackOverflow data and clean it for the LLM pipeline",
    "depend_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "capstonellm_pipeline_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["capstonellm"],
) as dag:
    start_dag = EmptyOperator(task_id="start_dag")

    ingest = DockerOperator(
        task_id="ingest",
        retries=0,
        image=IMAGE,
        container_name="capstonellm_ingest",
        api_version="auto",
        auto_remove="force",
        command=["capstonellm.tasks.ingest", "-t", TAG],
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        environment=AWS_ENV,
    )

    clean = DockerOperator(
        task_id="clean",
        image=IMAGE,
        container_name="capstonellm_clean",
        api_version="auto",
        auto_remove="force",
        command=["capstonellm.tasks.clean", "-e", "docker", "-t", TAG],
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        environment=AWS_ENV,
        trigger_rule="all_done"
    )

    verify_llm = BashOperator(
        task_id="verify_llm",
        bash_command=(
            'curl -sf -X POST "$LLM_VERIFY_URL" '
            '-H "Content-Type: application/json" '
            '-d \'{"query": "What is the equivalent of DataFrame.drop_duplicates() from pandas in polars?"}\''
        ),
        env={"LLM_VERIFY_URL": os.environ.get("LLM_VERIFY_URL", "")},
    )

    end_dag = EmptyOperator(task_id="end_dag")

    start_dag >> ingest >> clean >> verify_llm >> end_dag
