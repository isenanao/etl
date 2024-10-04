import pandas as pd
from datetime import datetime
import pathlib
import requests
import shutil
import psycopg2
import os
from sklearn.linear_model import LinearRegression
import shap
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler, OneHotEncoder


def telegram_bot_sendtext(bot_message):
   bot_token = '7110286756:AAEKF6ZWU4BWxJWdeQhWmz3TWSqTpSxa4wk'
   bot_chatID = '-1002089735568'
   send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message
   requests.get(send_text)

def check_data():
    pathlib.Path('data/sql/').mkdir(parents=True, exist_ok=True)
    source_df = pd.read_csv('data/parsed/data.csv', sep=';')
    cols = source_df.keys()
    source_df = source_df.reset_index(drop=True)
    new_df = pd.DataFrame(columns=[
        'price',
        'address',
        'url',
        'type',
        'metro',
        'metro_time',
        'lat',
        'log',
        'ad_region',
        'region', 
        'room',
        'm2',
        'floor',
        'max_floor'])
    diffs = []

    conn = psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format('ep-weathered-feather-a2ipxw3z.eu-central-1.aws.neon.tech', 5432, 'avito_vault', 'levch98', 'AlNR6sOQg7vP'))

    for _, row in source_df.iterrows():
        sql = '''
        SELECT 
        price,
        address,
        url,
        type,
        metro,
        metro_time,
        lat,
        log,
        ad_region,
        region, 
        room,
        m2,
        "floor",
        max_floor
        FROM l_flat AS f
        INNER JOIN h_house AS hh ON f.h_house_id = hh.id
        INNER JOIN s_house AS sh ON hh.id = sh.h_house_id
        INNER JOIN l_house_house_type lhht ON lhht.h_house_id = hh."id"
        INNER JOIN h_house_type hht ON lhht.h_house_type_id = hht.id
        INNER JOIN s_house_type sht ON sht.h_house_type_id = hht.id
        INNER JOIN l_house_geo lhhg ON lhhg.h_house_id = hh."id"
        INNER JOIN h_geo hhg ON lhhg.h_geo_id = hhg.id
        INNER JOIN s_geo shg ON shg.h_geo_id = hhg.id
        INNER JOIN h_advt AS ha ON ha."id" = f.h_advt_id
        INNER JOIN s_advt as sa ON sa.h_advt_id = ha."id"
        INNER JOIN h_metro as hm ON hm."id" = f.h_metro_id
        INNER JOIN s_metro as sm ON sm.h_metro_id = hm.id
        INNER JOIN h_flat_params as hfp ON hfp."id" = f.h_flat_params_id
        INNER JOIN s_flat_params as sfp ON sfp.h_flat_params_id = hfp."id"
        WHERE url = \'{}\''''.format(row['url'])

        db_df = pd.read_sql_query(sql, conn)
        db_df = db_df.reset_index(drop=True)

        if (len(db_df) == 0):
            new_df.loc[len(new_df)] = row
        else:
            separate_df = pd.DataFrame(row).transpose()
            separate_df.reset_index(drop=True, inplace=True)
        
            compared_df = db_df.compare(separate_df, keep_shape=True, keep_equal=True)

            for _, v in compared_df.iterrows():
                for col in cols:
                    if v[col]['self'] != v[col]['other']:
                        diffs.append({'url': v['url']['self'], 'col': col, 'new_value': v[col]['other']})

    new_df.to_csv('data/parsed/new.csv', sep=';', index=False)
    dups_df = pd.DataFrame(diffs, columns=['url', 'col', 'new_value'])
    dups_df.to_csv('data/parsed/dups.csv', sep=';', index=False)

def load_dups():

    # Дубликаты с изменениями
    with open('data/sql/dups.sql', 'w', encoding='utf8') as f:
        f.write('SELECT pg_sleep(1)')


    dups = pd.read_csv('data/parsed/dups.csv', sep=';')

    sat_mapping = {
        's_metro': ['metro', 'metro_time'],
        's_flat_params': ['room', 'm2', 'floor'],
        's_house': ['max_floor', 'address', 'ad_region', 'region'],
        's_house_type': ['type'],
        's_geo': ['lat', 'log'],
        's_advt': ['price']
    }

    hub_mapping = {
        'h_metro': ['metro_id', 'processed_dttm', 'source'],
        'h_flat_params': ['flat_params_id', 'processed_dttm', 'source'],
        'h_house': ['house_id', 'processed_dttm', 'source'],
        'h_house_type': ['house_type_id', 'processed_dttm', 'source'],
        'h_geo': ['geo_id', 'processed_dttm', 'source'],
        'h_advt': ['url', 'processed_dttm', 'source']
    }

    # Понять какой сателит и хаб

    for _, row in dups.iterrows():
        sat = ''
        hub = ''
        for k, v in enumerate(sat_mapping):
            if row['col'] in v:
                sat = k
                hub = list(k)
                hub[0] = 'h'
                hub = ''.join(hub)

        # Записать в сателит new_value в колонку col
        s_record = []
        for i in sat_mapping[sat]:
            if row['col'] == i:
                s_record.append(row['new_value'])
            else:
                # TODO: старые значения без изменений
                s_record.append('-')

        # Вычислить хэш
        h_hash = hash(tuple(s_record))

        #TODO: Источники могут быть разные
        h_record = [h_hash, datetime.now(), 'm2']

        with open('data/sql/dups.sql', 'a', encoding='utf8') as f:
            # Добавление хаба
            f.write('INSERT INTO ' + hub + ' (' + ','.join(hub_mapping[hub]) + ') VALUES ' + str(tuple(h_record)) + ';\n')
            # Добавление сателита
            f.write('INSERT INTO ' + sat + ' (' + ','.join(sat_mapping[sat]) + ') VALUES ' + str(tuple(s_record)) + ';\n')
        
def update_links():
    # Обновление линков 
    dups = pd.read_csv('data/parsed/dups.csv', sep=';')

    conn = psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format('ep-weathered-feather-a2ipxw3z.eu-central-1.aws.neon.tech', 5432, 'avito_vault', 'levch98', 'AlNR6sOQg7vP'))

    link_mapping = {
        'l_flat': ['h_metro', 'h_flat_type', 'h_house', 'h_advt'],
        'l_house_house_type': ['h_house_type'],
        'l_house_geo': ['h_geo']
    }

    for _, row in dups.iterrows():

        sql = '''SELECT 
                url,
                f."id" as l_flat_id, 
                lhht."id" as l_house_house_type_id, 
                lhhg."id" as L_house_geo_id
                FROM l_flat AS f
                INNER JOIN h_house AS hh ON f.h_house_id = hh.id
                INNER JOIN l_house_house_type lhht ON lhht.h_house_id = hh."id"
                INNER JOIN l_house_geo lhhg ON lhhg.h_house_id = hh."id"
                INNER JOIN h_advt AS ha ON ha."id" = f.h_advt_id
                WHERE url = \'{}\''''.format(row['url'])

        link_df = pd.read_sql_query(sql, conn)[0]
 
        for link, hubs in enumerate(link_mapping):
            for hub in hubs:
                # получить последнюю запись хаба
                sql = '''SELECT * FROM ''' + hub + ''' ORDER BY id DESC LIMIT 1'''

                db_df = pd.read_sql_query(sql, conn)

                hub_id = db_df['id'][0]

                #подставить его id в линк
                with open('data/sql/dups.sql', 'a', encoding='utf8') as f:
                    f.write('UPDATE ' + link + ' SET ' + hub  + '_id = ' + hub_id + ' WHERE id = ' + link_df[link + '_id'])




def load_new_data():

    # Новая запись

    data = pd.read_csv('data/parsed/new.csv', sep=';')
    data['processed_dttm'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    data['source'] = 'm2'

    s_metro = data[['metro', 'metro_time', 'source', 'processed_dttm']]
    s_metro.to_csv('data/s_metro.csv', index=False)

    s_metro['metro_id'] = pd.Series((hash(tuple(row)) for _, row in s_metro.iterrows()))

    h_metro = s_metro[['metro_id', 'source', 'processed_dttm']]
    h_metro.to_csv('data/h_metro.csv', index=False)

    #----

    s_flat_params = data[['room', 'm2', 'floor', 'source', 'processed_dttm']]
    s_flat_params.to_csv('data/s_flat_params.csv', index=False)

    s_flat_params['flat_params_id'] = pd.Series((hash(tuple(row)) for _, row in s_flat_params.iterrows()))

    h_flat_params = s_flat_params[['flat_params_id', 'source', 'processed_dttm']]
    h_flat_params.to_csv('data/h_flat_params.csv', index=False)

    #----

    s_house = data[['max_floor', 'address', 'ad_region', 'region', 'source', 'processed_dttm']]
    s_house.to_csv('data/s_house.csv', index=False)

    s_house['house_id'] = pd.Series((hash(tuple(row)) for _, row in s_house.iterrows()))

    h_house = s_house[['house_id', 'source', 'processed_dttm']]
    h_house.to_csv('data/h_house.csv', index=False)

    #----

    s_house_type = data[['type', 'source', 'processed_dttm']]
    s_house_type.to_csv('data/s_house_type.csv', index=False)

    s_house_type['house_type_id'] = pd.Series((hash(tuple(row)) for _, row in s_house_type.iterrows()))

    h_house_type = s_house_type[['house_type_id', 'source', 'processed_dttm']]
    h_house_type.to_csv('data/h_house_type.csv', index=False)

    #----

    s_geo = data[['lat', 'log', 'source', 'processed_dttm']]
    s_geo.to_csv('data/s_geo.csv', index=False)

    s_geo['geo_id'] = pd.Series((hash(tuple(row)) for _, row in s_geo.iterrows()))
    s_geo['source'] = 'dadata'

    h_geo = s_geo[['geo_id', 'source', 'processed_dttm']]
    h_geo.to_csv('data/h_geo.csv', index=False)

    #----

    s_advt = data[['url', 'price', 'source', 'processed_dttm']]
    
    s_advt['source'] = 'm2'

    h_advt = s_advt[['url', 'source', 'processed_dttm']]
    h_advt.to_csv('data/h_advt.csv', index=False)


    s_advt = s_advt[['price', 'source', 'processed_dttm']]
    s_advt.to_csv('data/s_advt.csv', index=False)

    #----

    links = data[['source', 'processed_dttm']]
    links.to_csv('data/l_flat.csv', index=False)
    links.to_csv('data/l_house_house_type.csv', index=False)
    links.to_csv('data/l_geo.csv', index=False)
    links.to_csv('data/l_house_geo.csv', index=False)


create_hubs = '''
CREATE TABLE IF NOT EXISTS h_metro (
   id SERIAL PRIMARY KEY,
   metro_id VARCHAR(255) NOT NULL,
   processed_dttm VARCHAR(255) NOT NULL,
   source VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS h_flat_params (
   id SERIAL PRIMARY KEY,
   flat_params_id VARCHAR(255) NOT NULL,
   processed_dttm VARCHAR(255) NOT NULL,
   source VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS h_house (
   id SERIAL PRIMARY KEY,
   house_id VARCHAR(255) NOT NULL,
   processed_dttm VARCHAR(255) NOT NULL,
   source VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS h_house_type (
   id SERIAL PRIMARY KEY,
   house_type_id VARCHAR(255) NOT NULL,
   processed_dttm VARCHAR(255) NOT NULL,
   source VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS h_geo (
   id SERIAL PRIMARY KEY,
   geo_id VARCHAR(255) NOT NULL,
   processed_dttm VARCHAR(255) NOT NULL,
   source VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS h_advt (
   id SERIAL PRIMARY KEY,
   url VARCHAR(255) NOT NULL,
   processed_dttm VARCHAR(255) NOT NULL,
   source VARCHAR(255) NOT NULL
);
'''

create_satelites = '''
CREATE TABLE IF NOT EXISTS s_metro (
   h_metro_id SERIAL PRIMARY KEY,
   metro VARCHAR(255) NOT NULL,
   metro_time DECIMAL NOT NULL,
   processed_dttm VARCHAR(255) NOT NULL,
   source VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS s_flat_params (
    h_flat_params_id SERIAL PRIMARY KEY,
    room DECIMAL NOT NULL,
    m2 DECIMAL NOT NULL,
    floor INT NOT NULL,
    processed_dttm VARCHAR(255) NOT NULL,
    source VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS s_house (
    h_house_id SERIAL PRIMARY KEY,
    max_floor INT NOT NULL,
    address VARCHAR(255) NOT NULL,
    ad_region VARCHAR(255) NOT NULL,
    region VARCHAR(255) NOT NULL,
    processed_dttm VARCHAR(255) NOT NULL,
    source VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS s_house_type (
    h_house_type_id SERIAL PRIMARY KEY,
    type VARCHAR(255) NOT NULL,
    processed_dttm VARCHAR(255) NOT NULL,
    source VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS s_geo (
    h_geo_id SERIAL PRIMARY KEY,
    lat DECIMAL NOT NULL,
    log DECIMAL NOT NULL,
    processed_dttm VARCHAR(255) NOT NULL,
    source VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS s_advt (
    h_advt_id SERIAL PRIMARY KEY,
    price DECIMAL NOT NULL,
    processed_dttm VARCHAR(255) NOT NULL,
    source VARCHAR(255) NOT NULL
);
'''

create_links = '''
CREATE TABLE IF NOT EXISTS l_house_geo (
   id SERIAL PRIMARY KEY,
   h_geo_id SERIAL NOT NULL,
   h_house_id SERIAL NOT NULL,
   processed_dttm VARCHAR(255) NOT NULL,
   source VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS l_house_house_type (
   id SERIAL PRIMARY KEY,
   h_house_type_id SERIAL NOT NULL,
   h_house_id SERIAL NOT NULL,
   processed_dttm VARCHAR(255) NOT NULL,
   source VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS l_flat (
   id SERIAL PRIMARY KEY,
   h_advt_id SERIAL NOT NULL,
   h_metro_id SERIAL NOT NULL,
   h_flat_params_id SERIAL NOT NULL,
   h_house_id SERIAL NOT NULL,
   processed_dttm VARCHAR(255) NOT NULL,
   source VARCHAR(255) NOT NULL
)
'''

create_metrics = '''
CREATE TABLE IF NOT EXISTS metrics (
   id SERIAL PRIMARY KEY,
   metro_minute DECIMAL NOT NULL,
   processed_dttm VARCHAR(255) NOT NULL
);'''

create_shap_table = '''
DROP TABLE IF EXISTS shap_metrics;
CREATE TABLE IF NOT EXISTS shap_metrics (
   id SERIAL PRIMARY KEY,
   feature_name VARCHAR(255) NOT NULL,
   sort BIGINT NOT NULL
);'''

def load_hubs():
    hubs = ['h_advt', 'h_metro', 'h_house_type', 'h_house', 'h_flat_params', 'h_geo']

    for hub in hubs:
        data = pd.read_csv('data/' + hub + '.csv')

        with open('data/sql/hubs.sql', 'a', encoding='utf8') as f:
            for _, d in data.iterrows():
                f.write('INSERT INTO ' + hub + ' (' + ','.join(data.columns) + ') VALUES ' + str(tuple(d.values.tolist())) + ';\n')



def load_sats():
    sats = ['s_advt', 's_metro', 's_house_type', 's_house', 's_flat_params', 's_geo']

    for sat in sats:
        data = pd.read_csv('data/' + sat + '.csv')

        with open('data/sql/sats.sql', 'a', encoding='utf8') as f:
            for _, d in data.iterrows():
                f.write('INSERT INTO ' + sat + ' (' + ','.join(data.columns) + ') VALUES ' + str(tuple(d.values.tolist())) + ';\n')

def load_links():
    links = ['l_house_geo', 'l_house_house_type', 'l_flat']

    for link in links:
        data = pd.read_csv('data/' + link + '.csv')

        with open('data/sql/links.sql', 'a', encoding='utf8') as f:
            for _, d in data.iterrows():
                f.write('INSERT INTO ' + link + ' (' + ','.join(data.columns) + ') VALUES ' + str(tuple(d.values.tolist())) + ';\n')

def clear_data():
    if  os.path.isdir('data'):
        shutil.rmtree("data/")

def get_iqr(df):

    Q1 = df['metro_time'].quantile(0.25)
    Q3 = df['metro_time'].quantile(0.75)
    IQR = Q3 - Q1

    return df.query('(@Q1 - 1.5 * @IQR) <= metro_time <= (@Q3 + 1.5 * @IQR)')



def get_metro_price():

    conn = psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format('ep-weathered-feather-a2ipxw3z.eu-central-1.aws.neon.tech', 5432, 'avito_vault', 'levch98', 'AlNR6sOQg7vP'))
    sql = '''
        SELECT 
        price,
        metro_time
        FROM l_flat AS f
        INNER JOIN h_advt AS ha ON ha."id" = f.h_advt_id
        INNER JOIN s_advt as sa ON sa.h_advt_id = ha."id"
        INNER JOIN h_metro as hm ON hm."id" = f.h_metro_id
        INNER JOIN s_metro as sm ON sm.h_metro_id = hm.id
        '''

    db_df = pd.read_sql_query(sql, conn)
    db_df = db_df.reset_index(drop=True)

    processed_df = get_iqr(db_df)
    
    X = processed_df[['metro_time']]
    y = processed_df['price']

    model = LinearRegression()
    model.fit(X, y)

    data = [abs(model.coef_[0]), datetime.today().strftime('%Y-%m-%d %H:%M:%S')]

    with open('data/sql/metrics.sql', 'a', encoding='utf8') as f:
        f.write('INSERT INTO metrics (metro_minute, processed_dttm) VALUES ' + str(tuple(data)) + ';\n')

def get_importances():

    conn = psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format('ep-weathered-feather-a2ipxw3z.eu-central-1.aws.neon.tech', 5432, 'avito_vault', 'levch98', 'AlNR6sOQg7vP'))
    sql = '''
        SELECT 
        price,
        address,
        url,
        type,
        metro,
        metro_time,
        lat,
        log,
        ad_region,
        region, 
        room,
        m2,
        "floor",
        max_floor
        FROM l_flat AS f
        INNER JOIN h_house AS hh ON f.h_house_id = hh.id
        INNER JOIN s_house AS sh ON hh.id = sh.h_house_id
        INNER JOIN l_house_house_type lhht ON lhht.h_house_id = hh."id"
        INNER JOIN h_house_type hht ON lhht.h_house_type_id = hht.id
        INNER JOIN s_house_type sht ON sht.h_house_type_id = hht.id
        INNER JOIN l_house_geo lhhg ON lhhg.h_house_id = hh."id"
        INNER JOIN h_geo hhg ON lhhg.h_geo_id = hhg.id
        INNER JOIN s_geo shg ON shg.h_geo_id = hhg.id
        INNER JOIN h_advt AS ha ON ha."id" = f.h_advt_id
        INNER JOIN s_advt as sa ON sa.h_advt_id = ha."id"
        INNER JOIN h_metro as hm ON hm."id" = f.h_metro_id
        INNER JOIN s_metro as sm ON sm.h_metro_id = hm.id
        INNER JOIN h_flat_params as hfp ON hfp."id" = f.h_flat_params_id
        INNER JOIN s_flat_params as sfp ON sfp.h_flat_params_id = hfp."id"
    '''

    data = pd.read_sql_query(sql, conn)
    data = data.reset_index(drop=True)


    categorical_features = ['metro', 'type', 'ad_region', 'region']
    numerical_features = ['price', 'metro_time', 'room', 'm2', 'floor', 'max_floor', 'log', 'lat']

    encoder = OneHotEncoder(sparse=False, drop='first')
    encoded_cat_features = encoder.fit_transform(data[categorical_features])
    encoded_feature_names = encoder.get_feature_names_out(categorical_features)
    encoded_cat_features = pd.DataFrame(encoded_cat_features, columns=encoded_feature_names)
    data_processed = pd.concat([data[numerical_features], encoded_cat_features], axis=1)


    X = data_processed.drop(columns=['price'])
    y = data_processed['price']

    model = GradientBoostingRegressor(random_state=42)
    model.fit(X, y)

    explainer = shap.Explainer(model)
    shap_values = explainer.shap_values(X)

    vals = np.abs(shap_values).mean(0)
    feature_names = X.columns

    result = list(zip(feature_names, vals))

    
    with open('data/sql/metrics.sql', 'a', encoding='utf8') as f:
            for r in result:
                f.write('INSERT INTO shap_metrics (feature_name, sort) VALUES ' + str(r) + ';\n')
    


def send_success():
    telegram_bot_sendtext('Успешно получены новые квартиры')

def send_fail():
    telegram_bot_sendtext('Произошла ошибка')
