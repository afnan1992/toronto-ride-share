from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import os
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


dag = DAG(
    'toronto_ride_share_etl',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
  
    description='ETL DAG',
    start_date=datetime(2024, 4, 2),
    max_active_runs= 1
    
) 

ExtractRides = DockerOperator (
        dag = dag,
        task_id="extract_ride_share",
        image='afnan1992/toronto-ride-share:latest',
        command='python rides_extract.py',
        docker_url='tcp://docker-proxy:2375',
        #  mounts=[
        # Mount(source='/',
        #       target='/code',
        #       type='bind'),],
        # network_mode='host',
        
    )

ExtractWeather = DockerOperator (
        dag = dag,
        task_id="extract_weather",
        image='afnan1992/toronto-ride-share:latest',
        command='python weather_extract.py',
        docker_url='tcp://docker-proxy:2375',
        #  mounts=[
        # Mount(source='/',
        #       target='/code',
        #       type='bind'),],
        # network_mode='host',
        
    )

load_parquet = GCSToBigQueryOperator(
    dag = dag,
    task_id="gcs_to_biguqery",
    bucket="toronto-ride-share-files",
    source_objects=["*.parquet"],
    destination_project_dataset_table=f"ride_share.rides",
    autodetect = True,
    source_format="PARQUET"
    # schema_fields=[
    #     {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    #     {"name": "post_abbr", "type": "STRING", "mode": "NULLABLE"},
    # ],
    # write_disposition="WRITE_TRUNCATE",
)


[ExtractRides,ExtractWeather] >> load_parquet 
