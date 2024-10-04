from bs4 import BeautifulSoup
import requests
import os
import pandas as pd
import pathlib
import re
from datetime import datetime

def get_geo(address):
    result = requests.post(
        'https://cleaner.dadata.ru/api/v1/clean/address',
        json=[address],
        headers={
            "Authorization" : "Token 7a9168d4e0e1883decad13f9003f1f28f70240c4",
            "X-Secret": "b1d470b2206ddaa5ae1e0cc3ef11c94a52aacea1"
        }
    ).json()


    data = {
        'lat': 0,
        'log': 0,
        'source': 'dadata',
        'processed_dttm': datetime.today().strftime('%Y-%m-%d %H:%M:%S') 
    }

    if result[0]['geo_lat'] is None and result[0]['geo_lon'] is None:
        return data
    
    data['lat'] = result[0]['geo_lat']
    data['log'] = result[0]['geo_lon']
    return data        




def get_links():
    pathlib.Path('data/parsed/').mkdir(parents=True, exist_ok=True)
    url = 'https://m2.ru/moskva/nedvizhimost/kupit-kvartiru/'

    r = requests.get(url)

    soup = BeautifulSoup(r.text)

    advts = soup.find_all('div', {'class': 'LayoutSnippet__generalInfo'})
    advts_parsed = []
    for a in advts:
        link = a.find_all("a", {'class': 'LinkSnippet LinkSnippet_hover'})[0]['href']
        tpy = a.find_all("div", {'data-test': 'snippet-mortgage-link'})[0].text

        if 'вторичку' in tpy:
            advts_parsed.append({'link': link, 'type': 'Вторичка'})
        else:
            advts_parsed.append({'link': link, 'type': 'Новостройка'})

    df = pd.DataFrame(advts_parsed)
    df.to_csv('data/parsed/links.csv')

def get_flats():
    links = pd.read_csv('data/parsed/links.csv')

    regex = re.compile('.*colors-named-module__secondary.*')

    flats = []

    for _, row in links.iterrows():
        flat = {}

        r = requests.get(row['link'])

        soup = BeautifulSoup(r.text)

        flat = {
            'price': soup.find_all("span", {'data-test': "offer-price"})[0].text,
            'address': [i.text for i in soup.find_all("a", {'data-test': 'offer-address'})],
            'url': row['link'],
            'type': row['type'],
            'metro': soup.find_all("a", {'data-test':"subway-link"})[0].text,
            'metro_time': soup.find_all("div", {'class': 'OfferRouteTimeCard'})[0].find_all("div", {'class': regex})[0].text
        }

        flat['address'] = ' '.join(flat['address'])

        geo = get_geo(flat['address'])
        flat['lat'] = geo['lat']
        flat['log'] = geo['log']

        flat['price'] = int(flat['price'].replace('\xa0', ''))
        flat['ad_region'] = flat['address'][1]
        flat['region'] = flat['address'][2]
        flat['metro_time'] = int(flat['metro_time'].replace('мин.', '').replace(' ', ''))


        for item in soup.find_all('div', {'class': 'DescriptionCell OfferCard__infoCell'}):
            for i in [{'find': 'Комнатность', 'key': 'rooms'}, {'find': 'Площадь квартиры', 'key': 'm2'}, {'find': 'Этаж', 'key': 'floor'}]:

                sub_item = item.find_all('div', {'class': 'DescriptionCell__body'})[0]

                item_key = sub_item.find_all('div', {'data-test': 'infoItemTitle'})[0].text
                item_value = sub_item.find_all('div', {'data-test': 'infoItemValue'})[0].text


                if i['find'] in item_key:

                    if i['key'] == 'floor':
                        floor_string = item_value.split('из')
                        flat['floor'] = floor_string[0].strip()
                        flat['max_floor'] = floor_string[1].strip()
                    elif i['key'] == 'rooms':
                        if '-' in item_value:
                            room_string = item_value.split('-')
                            flat['room'] = room_string[0]
                        else:
                            flat['room'] = item_value
                    else:
                        flat[i['key']] = item_value.replace('\xa0м²', '').replace(',', '.')

        if 'room' not in flat:
            flat['room'] = 0.5

        flats.append(flat)
    
    df = pd.DataFrame(flats)
    filename = 'data/parsed/data.csv'
    df.to_csv(filename, index=False, sep=';')