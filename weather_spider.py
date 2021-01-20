import scrapy
import pandas as pd
from urllib.parse import urlencode
import re

df = pd.read_pickle('counties.pkl')

params = [p.strip() for p in re.findall(
"\S+\s{3}",
"""
WS10M_MIN      MERRA2 1/2x1/2 Minimum Wind Speed at 10 Meters (m/s) 
QV2M           MERRA2 1/2x1/2 Specific Humidity at 2 Meters (g/kg) 
T2M_RANGE      MERRA2 1/2x1/2 Temperature Range at 2 Meters (C) 
WS10M          MERRA2 1/2x1/2 Wind Speed at 10 Meters (m/s) 
T2M            MERRA2 1/2x1/2 Temperature at 2 Meters (C) 
WS50M_MIN      MERRA2 1/2x1/2 Minimum Wind Speed at 50 Meters (m/s) 
T2M_MAX        MERRA2 1/2x1/2 Maximum Temperature at 2 Meters (C) 
WS50M          MERRA2 1/2x1/2 Wind Speed at 50 Meters (m/s) 
TS             MERRA2 1/2x1/2 Earth Skin Temperature (C) 
WS50M_RANGE    MERRA2 1/2x1/2 Wind Speed Range at 50 Meters (m/s) 
WS50M_MAX      MERRA2 1/2x1/2 Maximum Wind Speed at 50 Meters (m/s) 
WS10M_MAX      MERRA2 1/2x1/2 Maximum Wind Speed at 10 Meters (m/s) 
WS10M_RANGE    MERRA2 1/2x1/2 Wind Speed Range at 10 Meters (m/s) 
PS             MERRA2 1/2x1/2 Surface Pressure (kPa) 
T2MDEW         MERRA2 1/2x1/2 Dew/Frost Point at 2 Meters (C) 
T2M_MIN        MERRA2 1/2x1/2 Minimum Temperature at 2 Meters (C) 
T2MWET         MERRA2 1/2x1/2 Wet Bulb Temperature at 2 Meters (C) 
PRECTOT        MERRA2 1/2x1/2 Precipitation (mm day-1) 
"""
)]

def get_weather_url(fips):
    fips_county = df[df['FIPS']==fips]
    lat, lon = fips_county['Latitude'].values[0], fips_county['Longitude'].values[0]
    return (
        'https://power.larc.nasa.gov/cgi-bin/v1/DataAccess.py?' +
        urlencode({
            'request': 'execute',
            'tempAverage': 'DAILY',
            'identifier': 'SinglePoint',
            'parameters': ','.join(params),
            'userCommunity': 'SB',
            'lon': lon,
            'lat': lat,
            'startDate': 20000101,
            'endDate': 20201231,
            'outputList': 'JSON',
        })
    )

urls = []

for i, row in df.iterrows():
    fips = row['FIPS']
    urls.append(get_weather_url(fips))
        

class WeatherSpider(scrapy.Spider):
    name = 'weather_spider'
    start_urls = urls
    
    custom_settings = {
        "CONCURRENT_REQUESTS_PER_DOMAIN": 20,
        "LOG_LEVEL": "INFO",
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 3,
    }

    def parse(self, response):
        yield response.json()