import requests
from datetime import datetime

STATIONS = [
    'USC00201492',
]

#define TIMESTAMP here so it's the same for every file pulled in a given run
TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

if __name__ == '__main__':

    for station in STATIONS:
        response = requests.get(f'https://www.ncei.noaa.gov/pub/data/ghcn/daily/hcn/{station}.dly')
        
        output_file_name = f'/Volumes/data/default/data/weather/{station}_{TIMESTAMP}.txt'
        print(output_file_name)

        with open(output_file_name, 'wb') as f:
            f.write(response.content)