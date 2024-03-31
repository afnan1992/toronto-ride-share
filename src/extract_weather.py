import requests
import wget



def fetch_from_cgc():
    
    years = ['2018','2019','2020','2021','2022','2023','2024']
    for year in years:
        url = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=31688&Year="+year+"&Month=10&Day=14&timeframe=2&submit=%20Download+Data"

        output_path = '/home/afnan/toronto-ride-share/data/weather_data/toronto-weather-'+year+'.csv'

        try:
        # Download the file
            filename = wget.download(url, out=output_path)
            print(f"File downloaded: {filename}")
        except Exception as e:
            print(f"An error occurred: {e}")

fetch_from_cgc()

