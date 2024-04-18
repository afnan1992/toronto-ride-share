import os
import re
from google.cloud.storage import Client, transfer_manager
from google.cloud import storage

storage_client = storage.Client.from_service_account_json('/code/keys/key-1.json')
#storage_client = storage.Client.from_service_account_json('/home/afnan/toronto-ride-share/keys/key-1.json')

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request to upload is aborted if the object's
    # generation number does not match your precondition. For a destination
    # object that does not yet exist, set the if_generation_match precondition to 0.
    # If the destination object already exists in your bucket, set instead a
    # generation-match precondition using its generation number.
    generation_match_precondition = 0

    #blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)
    blob.upload_from_filename(source_file_name)
    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

def create_newfolder(bucket_name, destination_folder_name):
    
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_folder_name)

    blob.upload_from_string('')

    print('Created {} .'.format(destination_folder_name))


def move_files_to_google_cloud_storage(directory,gcspath,extension):
    
    #directory = '../code/data/extract/'
    #directory = '/home/afnan/toronto-ride-share/data/extract'
    
    files = os.listdir(directory)
    print(files)
    index = 0
    while index < len(files):
        filename = files[index]
        m = re.search(r"\b\d{4}\b", filename)
    
        if filename.endswith(extension) and m and int(m.group()) >= 2024:

            #create_newfolder("toronto-ride-share-files",str(m.group())+"/")
            if extension == '.parquet':
                upload_blob("toronto-ride-share-files",os.path.join(directory, filename),str(gcspath+'/'+m.group())+"/"+filename)
            else:
                upload_blob("toronto-ride-share-files",os.path.join(directory, filename),str(gcspath)+"/"+str(filename))
                

        index += 1




#create_newfolder("toronto-ride-share-files",str(m.group())+"/")










