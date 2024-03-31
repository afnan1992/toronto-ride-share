
import requests
from bs4 import BeautifulSoup
import logging
import configparser
import zipfile
from os import listdir
import os
import shutil
import sys
from pathlib import Path
import pyarrow.csv as pv
import pyarrow.parquet as pq
import re 


con =configparser.ConfigParser()
con.read('config.cfg')
    
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

def scrape_download_links()-> list:
    response = requests.get("https://ckan0.cf.opendata.inter.prod-toronto.ca/tr/dataset/bike-share-toronto-ridership-data")
    soup = BeautifulSoup(response.text, 'html.parser')
    download_tags = soup.find_all("a", {"class": "resource-url-analytics"})

    url_list = []

    for tag in download_tags:
        url_list.append(tag['href'])
    
    logging.info("scraped all links")
    return url_list

def download_zip_files(url_list:list):
   
    for url in url_list:
        
        logging.info("downloading file from the following url:"+url)
        output_path = con['DEFAULT']['output'] + url.rsplit('/', 1)[-1]
    
        r = requests.get(url)
        with open(output_path, 'wb') as f:
            f.write(r.content)
    logging.info("download zip files done")

def find_filenames(relative_path="",extension=".zip")->list:
       
    filenames = listdir(con['DEFAULT']['output']+"/"+relative_path)
    
    
    return [ con['DEFAULT']['output']+'/'+relative_path+'/'+filename for filename in filenames if filename.endswith(extension) ]

def unpack_zip(zip_file_names:list):
    for zip in zip_file_names:
        
        with zipfile.ZipFile(zip,"r") as zip_ref:
            zip_ref.extractall(con['DEFAULT']['output']+"/extract/")
    
    filenames = find_filenames("extract",".zip")

    for file in filenames:
        if file.endswith(".zip"):
            with zipfile.ZipFile(file,"r") as zip_ref:
                zip_ref.extractall(con['DEFAULT']['output'])
    
    
def change_structure() -> None:
    
    current_folder = con['DEFAULT']['output']+"extract/"
    parent_dir = Path.cwd() / current_folder
    for folder in parent_dir.iterdir():
        if not folder.is_dir():
            continue

        for child in folder.iterdir():
            if not child.name.startswith('.'):
                child.rename(parent_dir / child.name)
        shutil.rmtree(folder)
    logging.info("All folders removed")



def convert_to_parquet():
    file_names = find_filenames("/extract",".csv")
    for file in file_names:
        print("converting file"+file)
        try:
            
            table = pv.read_csv(file)
            pq.write_table(table, file.replace('csv', 'parquet'))
        except BaseException as e:
            print(str(e))
            # fix_file(file,str(e))

# def fix_file(file:str,exception:str)->None:
#     x = re.findall("^[0-9]{7,11}$", exception)
#     print(x)
#     df = pd.read_csv(file)
#     df['Trip  Duration'].loc[df['Trip Id'] == x[0]] = df['Trip Id'].astype(str).str[-3:]
#     df['Trip  Duration'].loc[df['Trip Id'] == x[0]] = df['Trip Id'].astype(str).str[:-3]
#     df.to_parquet(file+"fixed")

def main():
    url_list = scrape_download_links()
    download_zip_files(url_list)
    zip_file_names = find_filenames("",".zip")
    unpack_zip(zip_file_names)
    change_structure()
    convert_to_parquet()

main()

