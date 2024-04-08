from google.cloud import bigquery

#Trip Id	Trip  Duration	Start Station Id	Start Time	Start Station Name	End Station Id	End Time	End Station Name	Bike Id	User Type

# Construct a BigQuery client object.
def create_staging_table():
    client = bigquery.Client()

    
    table_id = "toronto-ride-share-pipeline.ride_share.rides"
    


    schema = [
        bigquery.SchemaField("Trip Id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("Trip Duration", "INTEGER"),
        bigquery.SchemaField("Start Station Id", "INTEGER"),
        bigquery.SchemaField("Start Time", "STRING"),
        bigquery.SchemaField("Start Station Name", "STRING"),
        bigquery.SchemaField("End Station Id", "INTEGER"),
        bigquery.SchemaField("End Time", "STRING"),
        bigquery.SchemaField("End Station Name", "STRING"),
        bigquery.SchemaField("Bike Id", "Integer"),
        bigquery.SchemaField("User Type", "STRING"),

    ]

    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

create_staging_table()

