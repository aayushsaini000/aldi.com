import traceback
import re
import csv
import json
import time
import scrapy
import requests
from lxml.html import fromstring
from scrapy.crawler import CrawlerProcess
from uszipcode import SearchEngine
from bs4 import BeautifulSoup

# PROXY = '37.48.118.90:13042'
PROXY = "3.221.94.217:80"

def get_states():
    return [
        "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
        "Connecticut", "Delaware", "District of Columbia", "Florida",
        "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas",
        "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts",
        "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana",
        "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico",
        "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma",
        "Oregon", "Pennsylvania", "Puerto Rico", "Rhode Island",
        "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah",
        "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin",
        "Wyoming"
    ]

def get_zip_codes_map():
    search = SearchEngine()
    zipcodes = list()
    for state in get_states():
    # for state in ['New York']:
        final_response = list()
        response = search.by_state(state, returns=2000)
        for r in response:
            if r.major_city not in [x.major_city for x in final_response]:
                final_response.append(r)
        for res in response:
            if res:
                zipcodes.append({
                    'zip_code': res.zipcode,
                    'latitude': res.lat,
                    'longitude': res.lng,
                    'city': res.major_city,
                    'state': res.state
                })
    return sorted(zipcodes, key=lambda k: k['state'])


class ExtractItem(scrapy.Item):
    ID = scrapy.Field()
    name = scrapy.Field()
    Opening_hours = scrapy.Field()
    Latitude = scrapy.Field()
    Longitude = scrapy.Field()
    Street = scrapy.Field()
    City = scrapy.Field()
    State = scrapy.Field()
    Zipcode = scrapy.Field()


class AldiStoreSpider(scrapy.Spider):
    name = "aldi_stores"
    allowed_domains = ["www.aldi.us"]
    scraped_data = list()
    fieldnames = [
        'ID', 'name', 'Opening_hours', 'Latitude', 'Longitude', 'Street',
        'City', 'State', 'Zipcode'
    ]

    def start_requests(self):
        base_url = "https://www.aldi.us/stores/en-us/Search?SingleSlotGeo=%s"
        headers = {
            "content-encoding": "gzip",
            "content-type": "text/html; charset=utf-8",
            "authority" : "www.aldi.us",
            "scheme": "https",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "accept-encoding" : "gzip, deflate, br",
            "accept-language" : "en-US,en;q=0.9",
            "cache-control" : "max-age=0",
            "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36"
        }
        zip_codes_map = get_zip_codes_map()
        for index, zip_code_map in enumerate(zip_codes_map, 1):
            url = base_url % zip_code_map['zip_code']
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                dont_filter=True,
                headers=headers
            )

    def parse(self, response):
        if not response.status == 200:
            return
        soup = BeautifulSoup(response.text, 'html.parser')
        stores = soup.findAll('li', {'class': 'resultItem clearfix'})

        if not stores:
            stores = []

        for store in stores:
            json_data = json.loads(store['data-json'])
            ref = json_data['id']
            lat = json_data['locY']
            lon = json_data['locX']
            name = store.find('strong', {'class': 'resultItem-CompanyName'}).text.strip()
            street = store.find('div', {'class': 'resultItem-Street'}).text.strip()
            try:
                address1 = store.find('div', {'class': 'resultItem-City'}).text.strip().split(',')
            except:
                address1 = ''
            hours_data = json_data['openingHours']

            pattern = r'(.*?)(\s|,\s)([0-9]{1,5})'
            if len(address1) == 2:
                city = address1[0]
                match = re.search(pattern, address1[1].strip())
                state = match.groups()[0]
                zipcode = match.groups()[2]

            elif len(address1) == 1:
                match = re.search(pattern, address1[0].strip())
                city, state, zipcode = match.groups()
            else:
                city = state = zipcode = ''

            hours_final = ''

            for hou in hours_data:
                hours_final += hou['day']['text'] + ': ' + hou['from'] + ' To ' + hou['until'] + ' & '

            hours_final = hours_final.strip(' & ').strip()

            if ref not in self.scraped_data:
                item = ExtractItem()
                item["ID"] = ref
                item["name"] = name
                item["Opening_hours"] = hours_final
                item["Latitude"] = lat
                item["Longitude"] = lon
                item["Street"] = street
                item["City"] = city
                item["State"] = state
                item["Zipcode"] = zipcode
                self.scraped_data.append(ref)
                yield item


def run_spider(no_of_threads, request_delay):
    settings = {
        "DOWNLOADER_MIDDLEWARES": {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
            'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
            'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
        },
        'ITEM_PIPELINES': {
            'pipelines.ExtractPipeline': 300,
        },
        'DOWNLOAD_DELAY': request_delay,
        'CONCURRENT_REQUESTS': no_of_threads,
        'CONCURRENT_REQUESTS_PER_DOMAIN': no_of_threads,
        'RETRY_HTTP_CODES': [403, 429, 500, 503],
        'ROTATING_PROXY_LIST': PROXY,
        'ROTATING_PROXY_BAN_POLICY': 'pipelines.BanPolicy',
        'RETRY_TIMES': 10,
        'LOG_ENABLED': True,

    }
    process = CrawlerProcess(settings)
    process.crawl(AldiStoreSpider)
    process.start()

if __name__ == '__main__':
    no_of_threads = 40
    request_delay = 0.01
    run_spider(no_of_threads, request_delay)
