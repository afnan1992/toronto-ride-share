from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import os


dag = DAG(
    'toronto_ride_share_etl',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
  
    description='ETL DAG',
    start_date=datetime(2024, 3, 28),
    max_active_runs= 1
    
) 

task_a = DockerOperator (
        dag = dag,
        task_id="extract_ride_share",
        image='afnan1992/toronto-ride-share:latest',
        command='python extract.py',
        docker_url='tcp://docker-proxy:2375',
        #  mounts=[
        # Mount(source='/',
        #       target='/code',
        #       type='bind'),],
        # network_mode='host',
        
    )
task_a 
