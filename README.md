# Toronto Ride Share

A fully automated datapipeline running in docker containers that utilizes GCP DataProc for transformation, Big Query for the cleaned data and Cloud Storage for storing the raw data. The purpose of this project was to understand how to utilize docker containers to fully automate the data pipeline process



# pre requisites 
    Have docker and docker compose installed
    Have a google cloud account
    
# How to Run
    Make sure that you have a folder named keys, with the following two service account json key files: cloud-data-storage-admin.json (to save files on cloud storage) and data-proc-admin.json( to run jobs on data proc cluster)
    Data proc admin service account should have the editor role as well so that it can create and destroy the cluster
    Go to the root directory of this project
    Run docker-compose build
    Run docker-compose up
