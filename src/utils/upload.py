
from utils.utils import upload_directory_with_transfer_manager,create_newfolder, upload_blob

import os
import re



#Trip Id	Trip  Duration	Start Station Id	Start Time	Start Station Name	End Station Id	End Time	End Station Name	Bike Id	User Type


def move_files_to_google_cloud_storage():
    
    directory = '../data/extract'
    
    files = os.listdir(directory)
    index = 0
    while index < len(files):
        filename = files[index]
        m = re.search(r"\b\d{4}\b", filename)
    
        if filename.endswith('.parquet') and m and int(m.group()) >= 2020:

            #create_newfolder("toronto-ride-share-files",str(m.group())+"/")
            upload_blob("toronto-ride-share-files",os.path.join(directory, filename),str(m.group())+"/"+filename)

        index += 1
    

move_files_to_google_cloud_storage()









