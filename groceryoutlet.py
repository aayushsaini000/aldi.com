import csv
import json
import os
import re
import time

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

path = 'C://chromedriver.exe'
chrome_options = Options()
chrome_options.add_argument('--disable-extensions')
chrome_options.add_argument('--profile-directory=Default')
chrome_options.add_argument("--disable-infobars")
chrome_options.add_argument("--incognito")
chrome_options.add_argument("--disable-plugins-discovery")
chrome_options.add_argument("--start-maximized")
driver = webdriver.Chrome(options=chrome_options, executable_path=path)


def parse():
    data = driver.page_source

    soup = BeautifulSoup(data, 'html.parser')
    stores = soup.findAll('li', {'class': 'border-bottom my-4'})

    if not stores:
        stores = []

    print(len(stores))

    for store in stores:
        ref = store.findAll('a', {'class': 'btn btn-red btn-sm btn-block gtm-adstore'})[0]['href'].strip().split('/')[-1].strip()
        lat = store.findAll('a', {'class': 'btn btn-red btn-sm btn-block'})[0]['href'].strip()
        lon = lat
        name = store.find('h6', {'class': 'store-title mb-2'}).text.strip()
        street = store.find('address').contents[0].strip()
        print(street)
        add_ = store.find('address').text.strip().replace(street, '')

        city = add_.split(',')[0].strip()
        state = add_.split(',')[1].strip().split(' ')[0]
        zipcode = add_.split(',')[1].strip().split(' ')[-1]

        hours_final = store.findAll('div')[-4].text.strip() + ' & ' + store.findAll('div')[-3].text.strip() + ' & ' + \
                      store.findAll('div')[-2].text.strip()

        # properties = {
        #     'ref': ref,
        #     'name': name,
        #     'opening_hours': hours_final,
        #     'lat': lat,
        #     'lon': lon,
        #     'street': street,
        #     'city': city,
        #     'state': state,
        #     'postcode': zipcode
        # }

        with open(csv_file, 'a') as writeFile:
            writer = csv.writer(writeFile, lineterminator='\n')
            writer.writerow([ref, name, hours_final, lat, lon, street, city, state, zipcode])


csv_file = 'GroceryoutletStores.csv'
if not os.path.exists(csv_file):
    with open(csv_file, 'a') as writeFile:
        writer = csv.writer(writeFile, lineterminator='\n')
        writer.writerow(['ID', 'Name', 'Opening_hours', 'Latitude ', 'Longitude', 'Street', 'City', 'State', 'Zipcode'])

driver.get('https://groceryoutlet.com/store-locator')
time.sleep(20)

parse()
