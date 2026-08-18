import requests
from datetime import datetime

STATIONS = [
    'USC00201492',
]

TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

for station in STATIONS:
    response = requests.get(F'https://www.ncei.noaa.gov/pub/data/ghcn/daily/hcn/{station}.dly')
    
    with open(f'/Volumes/data/default/data/weather/{station}_{TIMESTAMP}.txt', 'wb') as f:
        f.write(response.content)