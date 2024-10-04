import pandas as pd
from datetime import datetime
import requests
import pathlib
import shutil
import numpy as np
import glob
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook

dims_sql = '''
    CREATE TABLE IF NOT EXISTS dim_district (
        id SERIAL PRIMARY KEY,
        district VARCHAR(20),
        processed_dttm DATE NOT NULL,
        source VARCHAR(20) NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dim_metro (
        id SERIAL PRIMARY KEY,
        metro_station VARCHAR(100),
        metro_time INTEGER,
        processed_dttm DATE NOT NULL,
        source VARCHAR(20) NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dim_text (
        id SERIAL PRIMARY KEY,
        description TEXT,
        processed_dttm DATE NOT NULL,
        source VARCHAR(20) NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dim_seller (
        id SERIAL PRIMARY KEY,
        seller VARCHAR(100),
        seller_type VARCHAR(100),
        processed_dttm DATE NOT NULL,
        source VARCHAR(20) NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dim_advt (
        id SERIAL PRIMARY KEY,
        active SMALLINT,
        url VARCHAR(255),
        processed_dttm DATE NOT NULL,
        source VARCHAR(20) NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dim_building (
        id SERIAL PRIMARY KEY,
        address VARCHAR(255),
        wall_material VARCHAR(255),
        max_floor INTEGER,
        type VARCHAR(50),
        processed_dttm DATE NOT NULL,
        source VARCHAR(20) NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dim_geo (
        id SERIAL PRIMARY KEY,
        lat DECIMAL,
        lon DECIMAL,
        processed_dttm DATE NOT NULL,
        source VARCHAR(20) NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dim_flat_params (
        id SERIAL PRIMARY KEY,
        floor INTEGER,
        rooms INTEGER,
        area DECIMAL,
        furniture SMALLINT,
        repair VARCHAR(80),
        kitchen_furniture SMALLINT,
        wcs INTEGER,
        washing_machine SMALLINT,
        ceil_height DECIMAL,
        processed_dttm DATE NOT NULL,
        source VARCHAR(20) NOT NULL
    );
'''

fact_table_sql = '''
    CREATE TABLE IF NOT EXISTS fact_flat (
        metro_id SERIAL,
        text_id SERIAL,
        seller_id SERIAL,
        advt_id SERIAL,
        building_id SERIAL,
        flat_params_id SERIAL,
        district_id SERIAL,
        geo_id SERIAL,
        price DECIMAL,
        processed_dttm DATE NOT NULL,
        source VARCHAR(20) NOT NULL
    )
'''

def get_urls():
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    df = hook.get_pandas_df(sql="select url from dim_advt;")
    df.to_csv('data/urls.csv')

def get_coords(address):
    result = requests.post(
        'https://cleaner.dadata.ru/api/v1/clean/address',
        json=[address],
        headers={
            "Authorization" : "Token 7a9168d4e0e1883decad13f9003f1f28f70240c4",
            "X-Secret": "b1d470b2206ddaa5ae1e0cc3ef11c94a52aacea1"
        }
    ).json()

    return result[0]['geo_lat'],  result[0]['geo_lon']

def enrich_coords():

    pathlib.Path('data/enrich/').mkdir(parents=True, exist_ok=True)

    all_files = glob.glob(os.path.join('data/parser', "flats_*.csv"))

    flats_df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)

    for i, record in flats_df.iterrows():
        lat, lon = get_coords(record['address'])
        flats_df.at[i,'lat'] = lat
        flats_df.at[i,'lon'] = lon

    flats_df.to_csv('data/enrich/flats.csv')

def process_values_for_sql(values):
    value_list = []

    for v in values:
        if str(v) == 'nan' or v == np.nan:
            value_list.append('NULL')
        elif type(v) == str:
            value_list.append(f"'{v}'")
        else:
            value_list.append(str(v))

    return ','.join(value_list)


def df_to_sql(table_name, df):
    columns = ','.join(df.columns)

    if 'dim' in table_name:
        path = 'data/sql/dims.sql'
    else:
        path = 'data/sql/facts.sql'

    with open(path, 'a+', encoding='utf8') as fl:
        for _, record in df.iterrows():
            fl.write(f'INSERT INTO {table_name} ({columns}) VALUES (' + process_values_for_sql(record.values.tolist()) + ');\n')


def create_flat_dims():
    flats_df = pd.read_csv('data/enrich/flats.csv')
    pathlib.Path('data/sql/').mkdir(parents=True, exist_ok=True)

    urls = pd.read_csv('data/urls.csv')['url'].tolist()

    flats_df = flats_df.query("url != @urls")

    flats_df['source'] = 'm2'
    flats_df['processed_dttm'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

    dim_text = flats_df[['description', 'processed_dttm', 'source']]
    df_to_sql('dim_text', dim_text)

    dim_district = flats_df[['district', 'processed_dttm', 'source']]
    df_to_sql('dim_district', dim_district)

    dim_metro = flats_df[['metro_station', 'metro_time', 'processed_dttm', 'source']]
    df_to_sql('dim_metro', dim_metro)

    dim_seller = flats_df[['seller', 'seller_type', 'processed_dttm', 'source']]
    df_to_sql('dim_seller', dim_seller)
    
    dim_advt = flats_df[['active', 'url', 'processed_dttm', 'source']]
    df_to_sql('dim_advt', dim_advt)

    dim_building = flats_df[
        [
            'address',
            'wall_material',
            'max_floor',
            'type',
            'processed_dttm',
            'source'
        ]
    ]

    df_to_sql('dim_building', dim_building)

    dim_flat_params = flats_df[
        [
            'floor',
            'rooms',
            'area',
            'furniture',
            'repair',
            'kitchen_furniture',
            'wcs',
            'washing_machine',
            'ceil_height',
            'processed_dttm',
            'source'
        ]
    ]

    df_to_sql('dim_flat_params', dim_flat_params)

    dim_geo = flats_df[['lat', 'lon', 'processed_dttm', 'source']]
    dim_geo['source'] = 'dadata'

    df_to_sql('dim_geo', dim_geo)

def create_flat_facts():
    pathlib.Path('data/sql/').mkdir(parents=True, exist_ok=True)

    flats_df = pd.read_csv('data/enrich/flats.csv')

    flats_df['source'] = 'm2'
    flats_df['processed_dttm'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

    fact_flats = flats_df[['price', 'processed_dttm', 'source']]
    df_to_sql('fact_flat', fact_flats)
    
def clear_data():
    pathlib.Path('data/').mkdir(parents=True, exist_ok=True)
    shutil.rmtree('data/')
