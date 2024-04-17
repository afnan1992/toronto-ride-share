from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import os
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash import BashOperator


from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator


PYSPARK_JOB = {
    "reference": {"project_id": 'toronto-ride-share-pipeline'},
    "placement": {"cluster_name": 'cluster-data-proc-afnan'},
    "pyspark_job": {"main_python_file_uri": 'gs://toronto-ride-share-files/code/transform.py'},
}

dag = DAG(
    'toronto_ride_share_etl',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
  
    description='ETL DAG',
    start_date=datetime(2024, 4, 12),
    max_active_runs= 1
    
) 

ExtractRides = DockerOperator (
        dag = dag,
        task_id="extract_ride_share",
        image='extract',
        command='python rides_extract.py',
        docker_url='tcp://docker-proxy:2375',
       
        
    )

ExtractWeather = DockerOperator (
        dag = dag,
        task_id="extract_weather",
        #image='afnan1992/toronto-ride-share:latest',
        image = 'extract',
        command='python weather_extract.py',
        docker_url='tcp://docker-proxy:2375',
       
        
    )

upload_to_gcs = LocalFilesystemToGCSOperator(
    task_id='upload_to_gcs',
    dag=dag,
    src='/opt/airflow/dags/transform.py',
    dst='code/transform.py',
    bucket = 'toronto-ride-share-files'
)


# Submit Spark job on Dataproc
pyspark_task = DataprocSubmitJobOperator(
    task_id="pyspark_task", 
    job=PYSPARK_JOB, 
    region='northamerica-northeast2',
    project_id='toronto-ride-share-pipeline',
    gcp_conn_id = 'gcp_data_proc'
)





delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
)


[ExtractRides,ExtractWeather] >> upload_to_gcs >> pyspark_task >> delete_cluster

