from bs4 import BeautifulSoup
import requests
import pandas as pd
import pathlib
import threading

advt_parsed = {'links': [], 'type': []}

map = {
  'Комнатность': 'rooms',
  'Площадь квартиры': 'area',
  'Этаж': 'floor',
  'Кол-во санузлов': 'wcs',
  'Ремонт': 'repair',
  'Мебель': 'furniture',
  'Кухонная мебель': 'kitchen_furniture',
  'Стиральная машина': 'washing_machine',
  'Высота потолков': 'ceil_height',
  'Материал стен': 'wall_material'
}

yorn_map = {
    'нет': 0,
    'есть': 1
}

flats_main_url = 'https://m2.ru/moskva/nedvizhimost/kupit-kvartiru/?pageNumber='

seller_types = ['Агент', 'Собственник', 'Застройщик']

feature = 'lxml'


def get_links():
    pathlib.Path('data/parser/').mkdir(parents=True, exist_ok=True)

    max_page = 15

    for i in range(max_page):
        url = flats_main_url + str(i + 1)

        r = requests.get(url)

        soup = BeautifulSoup(r.text, feature)

        for advt_general in soup.find_all('div', {'class': 'LayoutSnippet__generalInfo'}):

            advt = advt_general.find('a', {'class': 'LinkSnippet LinkSnippet_hover'})

            morgage = advt_general.find('div', {'data-test': 'snippet-mortgage-link'})

            if morgage is None:
                advt_parsed['type'].append('вторичка')
            elif 'вторичку' in morgage.text:
                advt_parsed['type'].append('вторичка')
            else:
                advt_parsed['type'].append('новостройка')

            advt_parsed['links'].append(advt['href'])

    df = pd.DataFrame(advt_parsed)
    df.to_csv('data/parser/links.csv')

def get_params(soup):

    titles = soup.find_all('div', {'data-test': 'infoItemTitle'})
    values = soup.find_all('div', {'data-test': 'infoItemValue'})

    params = {}

    params['rooms'] = '0.5'

    for title in map.keys():
        ts = [t.text for t in titles]

        if title in ts:
            value = values[ts.index(title)].text

            if title == 'Комнатность':
                value = int(value.replace('-комнатная', ''))

            if title == 'Площадь квартиры':
                value = float(value.replace('\xa0м²', '').replace(',', '.'))

            if title == 'Этаж':
                split_value = value.split(' из ')
                value = int(split_value[0])
                params['max_floor'] = int(split_value[1])

            if title in ['Мебель', 'Кухонная мебель', 'Стиральная машина']:
                value = yorn_map[value]

            if title == 'Высота потолков':
                value = float(value.replace(' м', '').replace(',', '.'))

            if title == 'wcs':
                value = int(value)

            params[map[title]] = value

    return params

def get_seller_data(soup):
    seller_info = soup.find_all('div', {'class': 'CardAuthorBadge__seller'})[0].text
    seller_data = {}

    for seller_type in seller_types:
        if seller_type in seller_info:
            seller_data['seller_type'] = seller_type
            seller_data['seller'] = seller_info.replace(seller_type, '')
    
    return seller_data

def get_flat(urls, thread_num):
    flats = []
    for k, url in urls.iterrows():

        flat = {}

        r = requests.get(url[1])

        soup = BeautifulSoup(r.text, feature)

        flat['url'] = url[1]

        flat['active'] = int(len(soup.find_all('div', {'class': 'OfferCard__deleted'})) == 0)

        flat['type'] = urls['type'][k]

        addr_string = soup.find_all('a', {'data-test': 'offer-address'})

        flat['address'] = ', '.join([i.text for i in addr_string])

        flat['district'] = addr_string[1].text

        flat['price'] = soup.find_all("span", {'data-test': "offer-price"})[0].text
        flat['price'] = flat['price'].replace('\xa0', '')

        flat['metro_station'] = soup.find_all("a", {'data-test': "subway-link"})[0].text
        flat['metro_time'] = soup.find_all("div", {
            'class': "colors-named-module__secondary___eb0c51 fonts-module__primary___73abfc"
        })[0].text
        flat['metro_time'] = flat['metro_time'].replace(' мин.', '')

        flat['description'] = soup.find_all("div", {'itemprop': "description"})[0].text

        params = get_params(soup=soup)
        seller_data = get_seller_data(soup=soup)

        flat = {**flat, **params, **seller_data}
        flats.append(flat)
    
    df = pd.DataFrame(flats)
    df.to_csv(f"data/parser/flats_{thread_num}.csv")

def get_flats():

    get_links()

    advt_parsed = pd.read_csv('data/parser/links.csv')

    n = 10

    chunks = [advt_parsed[i:i+n] for i in range(0,advt_parsed.shape[0],n)]

    for k, chunk in enumerate(chunks):
        threads = []
        
        thread = threading.Thread(target=get_flat, args=(chunk, k))
        threads.append(thread)

        for t in threads:
            t.start()

        for t in threads:
            t.join()
        





