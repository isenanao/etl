from selenium import webdriver
from bs4 import BeautifulSoup
from time import sleep
from selenium.webdriver.chrome.options import Options
from random import randint, choice
import pandas as pd
from datetime import datetime
import pathlib
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook

agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:103.0) Gecko/20100101 Firefox/103.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 12.5; rv:103.0) Gecko/20100101 Firefox/103.0',
    'Mozilla/5.0 (X11; Linux i686; rv:103.0) Gecko/20100101 Firefox/103.0',
    'Mozilla/5.0 (Linux x86_64; rv:103.0) Gecko/20100101 Firefox/103.0'
    'Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:103.0) Gecko/20100101 Firefox/103.0',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:103.0) Gecko/20100101 Firefox/103.0',
    'Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:103.0) Gecko/20100101 Firefox/103.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 12_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36 Edg/104.0.1293.47',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36 Edg/104.0.1293.47',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36 OPR/89.0.4447.71',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36 OPR/89.0.4447.71',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36 OPR/89.0.4447.71',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36 OPR/89.0.4447.71',
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36 Vivaldi/5.3.2679.70',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36 Vivaldi/5.3.2679.70',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36 Vivaldi/5.3.2679.70',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36 Vivaldi/5.3.2679.70',
    'Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36 Vivaldi/5.3.2679.70',
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 YaBrowser/22.7.0 Yowser/2.5 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 YaBrowser/22.7.0 Yowser/2.5 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 YaBrowser/22.7.0 Yowser/2.5 Safari/537.36',
]

def telegram_bot_sendtext(bot_message):
   bot_token = '7110286756:AAEKF6ZWU4BWxJWdeQhWmz3TWSqTpSxa4wk'
   bot_chatID = '-1002089735568'
   send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message
   requests.get(send_text)

def get_links():
    pathlib.Path('data/links/').mkdir(parents=True, exist_ok=True)
    url = 'https://www.avito.ru/moskva/kvartiry/prodam-ASgBAgICAUSSA8YQ?context=H4sIAAAAAAAA_0q0MrSqLraysFJKK8rPDUhMT1WyLrYyNLNSKk5NLErOcMsvyg3PTElPLVGyrgUEAAD__xf8iH4tAAAA&p=1&s=104'

    options = Options()
    options.add_argument("user-agent=" + choice(agents))
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-setuid-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--start-maximized")
    options.add_argument("--window-size=1920,1080")
    browser = webdriver.Remote('host.docker.internal:4444/wd/hub', options=options)

    browser.get(url)

    soup = BeautifulSoup(browser.page_source)

    mylinks = soup.find_all("a", {"itemprop": "url", "class": "styles-module-root-QmppR styles-module-root_noVisited-aFA10"})

    with open('data/links/links.txt', 'a', encoding='utf8') as f:
        for i in mylinks:
            f.write('https://www.avito.ru' + i['href'] + '\n')
    browser.quit()

def extract_new_flats():
    pathlib.Path('data/parsed/').mkdir(parents=True, exist_ok=True)

    with open('data/links/links.txt', 'r', encoding='utf8') as f:
        pages = f.readlines()

    options = Options()
    options.add_argument("user-agent=" + choice(agents))
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-setuid-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--start-maximized")
    options.add_argument("--window-size=1920,1080")
    options.page_load_strategy = 'eager'
    options.add_argument("user-agent=" + choice(agents))

    browser = webdriver.Remote('host.docker.internal:4444/wd/hub', options=options)

    for page in pages[:8]:
        try:
            f = open('data/parsed/data.csv', 'a', encoding='utf8')

            browser.get(page)

            sleep(randint(3, 8))

            browser.execute_script("window.scrollBy(0,"+ str(randint(14, 1000)) + ");")

            soup = BeautifulSoup(browser.page_source)

            flat = {
                'price': soup.find_all("span", {'class': 'styles-module-size_xxxl-A2qfi'})[0].text,
                'url': page.replace('\n', ''),
                'address': soup.find_all("span", {'class': 'style-item-address__string-wt61A'})[0].text,
                'type': soup.find_all("a", {'class': 'breadcrumbs-link-Vr4Nc'})[4].text,
                'metro': soup.find_all("span", {'class': 'style-item-address-georeferences-item-TZsrp'})[0].find_all('span')[1].text,
                'metro_time': soup.find_all("span", {'class': 'style-item-address-georeferences-item-interval-ujKs2'})[0].text
            }

            for item in soup.find_all('li', {'class': 'params-paramsList__item-_2Y2O'}):
                for i in [{'find': 'Количество комнат', 'key': 'room'}, {'find': 'Общая площадь', 'key': 'm2'}, {'find': 'Этаж', 'key': 'floor'}]:
                    item_key = item.find('span').text
                    if i['find'] in item_key:
                        if i['key'] == 'floor':
                            if 'из' in item.text:
                                floor_string = item.text.split('из')
                                flat['floor'] = floor_string[0].split(':')[1].strip()
                                flat['max_floor'] = floor_string[1].strip()
                        else:
                            flat[i['key']] = item.text.split(':')[1].strip().replace('\xa0м²', '')

            if 'до' in flat['metro_time']:
                flat['metro_time'] = flat['metro_time'].replace('до', '').replace('мин.', '').strip()
            else:
                metro_times = flat['metro_time'].replace('мин.', '').replace(' ', '').split('–')
            
                flat['metro_time'] = sum([int(i) for i in metro_times[0]]) / 2

            flat['price'] = flat['price'].replace('\xa0', '').replace('₽', '')
            f.write(";".join([str(i) for i in flat.values()]) + '\n')
            f.close()

        except Exception:
            continue
    pass

def transform_new_flats():
    columns = ['price', 'url', 'address', 'type', 'metro', 'metro_time', 'room' ,'m2', 'floor', 'max_floor']
    data = pd.read_csv('data/parsed/data.csv', header=None, names=columns, sep=';')

    data['source'] = 'avito'
    data['processed_dttm'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

    data_metro = data[['metro', 'metro_time', 'source', 'processed_dttm']]
    data_metro.to_parquet('data/metro')

    data_flat_params = data[['room', 'm2', 'floor', 'source', 'processed_dttm']]
    data_flat_params.to_parquet('data/flat_params')

    data_house = data[['type', 'max_floor', 'address', 'source', 'processed_dttm']]
    data_house.to_parquet('data/house')

    data_fact = data[['price', 'url']]
    data_fact.to_parquet('data/flats')


create_dim_metro = '''
    CREATE TABLE IF NOT EXISTS dim_metro (
    id SERIAL PRIMARY KEY,
    metro VARCHAR(255) NOT NULL,
    metro_time DECIMAL NOT NULL,
    processed_dttm VARCHAR(255) NOT NULL,
    source VARCHAR(255) NOT NULL
)'''

create_dim_flat_params = '''
    CREATE TABLE IF NOT EXISTS dim_flat_params (
    id SERIAL PRIMARY KEY,
    room VARCHAR(255) NOT NULL,
    m2 DECIMAL NOT NULL,
    floor INT NOT NULL,
    processed_dttm VARCHAR(255) NOT NULL,
    source VARCHAR(255) NOT NULL
)''' #room, m2, floor

create_dim_house = '''
    CREATE TABLE IF NOT EXISTS dim_house (
    id SERIAL PRIMARY KEY,
    type VARCHAR(255) NOT NULL,
    max_floor INT NOT NULL,
    address VARCHAR(255) NOT NULL,
    processed_dttm VARCHAR(255) NOT NULL,
    source VARCHAR(255) NOT NULL
)''' #type

create_dim_geo = '''
    CREATE TABLE IF NOT EXISTS dim_geo (
    id SERIAL PRIMARY KEY,
    house_id INT NOT NULL,
    lat DECIMAL NOT NULL,
    log DECIMAL NOT NULL,
    processed_dttm VARCHAR(255) NOT NULL,
    source VARCHAR(255) NOT NULL
)''' #type

create_fact_table = '''
    CREATE TABLE IF NOT EXISTS fact_flat (
    id SERIAL PRIMARY KEY,
    metro_id SERIAL,
    flat_params_id SERIAL,
    house_id SERIAL,
    price DECIMAL NOT NULL,
    url varchar(255) NOT NULL
);
create unique index if not exists url_unique_idx on fact_flat (url);
'''

def load_data(file, tablename, result_file):
    pathlib.Path('data/sql/').mkdir(parents=True, exist_ok=True)
    data = pd.read_parquet(file)
    with open(result_file, 'w+', encoding='utf8') as f:
        for _, d in data.iterrows():
            if 'fact' in tablename:
                f.write('INSERT INTO ' + tablename + ' (' + ','.join(data.columns) + ') VALUES ' + str(tuple(d.values.tolist())) + ' ON CONFLICT (url) DO NOTHING;\n')
            else:
                f.write('INSERT INTO ' + tablename + ' (' + ','.join(data.columns) + ') VALUES ' + str(tuple(d.values.tolist())) + ';\n')


def save_db():
    load_data('data/flats', 'fact_flat', 'data/sql/flat.sql')
    load_data('data/metro', 'dim_metro', 'data/sql/metro.sql')
    load_data('data/flat_params', 'dim_flat_params', 'data/sql/flat_params.sql')
    load_data('data/house', 'dim_house', 'data/sql/house.sql')

def clear_orphans():
    table_mapping = [
        {
            'table': 'dim_metro',
            'field': 'metro_id'
        },
        {
            'table': 'dim_flat_params',
            'field': 'flat_params_id'
        },
        {
            'table': 'dim_house',
            'field': 'house_id'
        }
    ]
    for table in table_mapping:
        with open('data/sql/' + table['table'] + '.sql', 'w+', encoding='utf8') as f:
            f.write('DELETE FROM ' + table['table'] + ' where id not in (select ' + table['field'] + ' from fact_flat)\n')

def get_nogeo():
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    df = hook.get_pandas_df(sql="select address, id FROM dim_house AS dh WHERE NOT exists (select 1 FROM dim_geo where house_id = dh.id);")
    df.to_parquet('data/geo')

def get_geo():
    data = pd.read_parquet('data/geo')
    with open('data/sql/geo.sql', 'w+', encoding='utf8') as f:
        for _, record in data.iterrows():
            result = requests.post(
                'https://cleaner.dadata.ru/api/v1/clean/address',
                json=[record['address']],
                headers={
                    "Authorization" : "Token 7a9168d4e0e1883decad13f9003f1f28f70240c4",
                    "X-Secret": "b1d470b2206ddaa5ae1e0cc3ef11c94a52aacea1"
                }
            ).json()

            if result[0]['geo_lat'] is None and result[0]['geo_lon'] is None:
                continue

            coords = {
                'house_id': record['id'],
                'lat':result[0]['geo_lat'],
                'log': result[0]['geo_lon'],
                'source': 'dadata',
                'processed_dttm': datetime.today().strftime('%Y-%m-%d %H:%M:%S')
            }

            f.write('INSERT INTO dim_geo (' + ','.join(coords.keys()) + ') VALUES ' + str(tuple(coords.values())) + ';\n')


def send_success():
    telegram_bot_sendtext('Успешно получены новые квартиры')

def send_fail():
    telegram_bot_sendtext('Произошла ошибка')
