from google.cloud import bigquery

# Construct a BigQuery client object.
def gcs_to_big_query():
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = "toronto-ride-share-pipeline.ride_share.rides"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=[
        bigquery.SchemaField("Trip_Id", "INT64"),
        bigquery.SchemaField("Trip_Duration", "FLOAT64"),
        bigquery.SchemaField("Start_Station_Id", "INT64"),
        bigquery.SchemaField("Start_Time", "STRING"),
        bigquery.SchemaField("Start_Station_Name", "STRING"),
        bigquery.SchemaField("End_Station_Id", "FLOAT64"),
        bigquery.SchemaField("End_Time", "STRING"),
        bigquery.SchemaField("End_Station_Name", "STRING"),
        bigquery.SchemaField("Bike_Id", "INT64"),
        bigquery.SchemaField("User_Type", "STRING"),
    ],

    )
    uri = "gs://toronto-ride-share-files/*.parquet"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

gcs_to_big_query()
