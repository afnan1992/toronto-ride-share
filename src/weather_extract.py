import requests
import wget

from utils.upload import move_files_to_google_cloud_storage

def fetch_from_cgc():
    
    years = ['2018','2019','2020','2021','2022','2023','2024']
    for year in years:
        url = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=31688&Year="+year+"&Month=10&Day=14&timeframe=2&submit=%20Download+Data"

        #output_path = '/home/afnan/toronto-ride-share/data/extract/weather_data/toronto-weather-'+year+'.csv'
        output_path = '/code/data/extract/weather_data/toronto-weather-'+year+'.csv'
        
        
        
        try:
        # Download the file
            filename = wget.download(url, out=output_path)
            print(f"File downloaded: {filename}")
        except Exception as e:
            print(f"An error occurred: {e}")

fetch_from_cgc()

print("moving weather files to gcs")
move_files_to_google_cloud_storage("../code/data/extract/weather_data/",'weather','.csv')
