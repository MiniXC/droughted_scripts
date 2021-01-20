import scrapy
import pandas as pd
from urllib.parse import urlencode
import re
import xmltodict

df = pd.read_pickle('counties.pkl')

def get_drought_url(fips):
    fips_county = df[df['FIPS']==fips]
    lat, lon = fips_county['Latitude'].values[0], fips_county['Longitude'].values[0]
    if len(str(fips)) == 4:
        fips = "0" + str(fips)
    return (
        'https://usdmdataservices.unl.edu/api/CountyStatistics/GetDroughtSeverityStatisticsByAreaPercent?' +
        urlencode({
            'aoi': fips,
            'startdate': '1/1/2000',
            'enddate': '12/31/2020',
            'statisticsType': 1,
        })
    )

urls = []

for i, row in df.iterrows():
    fips = row['FIPS']
    urls.append(get_drought_url(fips))
        

class WeatherSpider(scrapy.Spider):
    name = 'weather_spider'
    start_urls = urls
    
    custom_settings = {
        "CONCURRENT_REQUESTS_PER_DOMAIN": 20,
        "LOG_LEVEL": "INFO",
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1,
        "AUTOTHROTTLE_MAX_DELAY": 3,
    }


    def parse(self, response):
        yield xmltodict.parse(response.text)