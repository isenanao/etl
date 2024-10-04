import uuid
import random
import pathlib

def get_test_data():
    pathlib.Path('data/parsed/').mkdir(parents=True, exist_ok=True)
    data = {}
    data['price'] = random.randint(1000000, 300000000)
    data['url'] = '/moskva/flat' + str(random.randint(100, 300))
    data['address'] = uuid.uuid1()
    data['type'] = 'вторичка'
    data['metro'] = uuid.uuid1()
    data['metro_time'] = random.randint(1, 30)
    data['room'] = random.randint(1, 3)
    data['m2'] = random.randint(20, 90)
    data['floor'] = random.randint(1, 30)
    data['max_floor'] = random.randint(1, 30)
    data['lat'] = random.randint(-180, 180)
    data['log'] = random.randint(-180, 180)
    data['ad_region'] = uuid.uuid1()
    data['region'] = uuid.uuid1()

    f = open('data/parsed/data.csv', 'a', encoding='utf8')
    f.write(";".join([str(i) for i in data.values()]) + '\n')
    f.close()

def unchanged_dup():

    pathlib.Path('data/parsed/').mkdir(parents=True, exist_ok=True)
    data = {}
    data['price'] = 55951353
    data['url'] = '/moskva/flat157'
    data['address'] = '77ca3680-fcdd-11ee-97ed-0242ac150003'
    data['type'] = 'вторичка'
    data['metro'] = '77ca3b44-fcdd-11ee-97ed-0242ac150003'
    data['metro_time'] = 28
    data['room'] = 3
    data['m2'] = 72
    data['floor'] = 23
    data['max_floor'] = 1
    data['lat'] = 159
    data['log'] = 24
    data['ad_region'] = '77ca3d2e-fcdd-11ee-97ed-0242ac150003'
    data['region'] = '77ca3d88-fcdd-11ee-97ed-0242ac150003'

    f = open('data/parsed/data.csv', 'a', encoding='utf8')
    f.write(";".join([str(i) for i in data.values()]) + '\n')
    f.close()

def changed_dup():

    pathlib.Path('data/parsed/').mkdir(parents=True, exist_ok=True)
    data = {}
    data['price'] = 56000000
    data['url'] = '/moskva/flat157'
    data['address'] = '77ca3680-fcdd-11ee-97ed-0242ac150003'
    data['type'] = 'вторичка'
    data['metro'] = '77ca3b44-fcdd-11ee-97ed-0242ac150003'
    data['metro_time'] = 28
    data['room'] = 3
    data['m2'] = 72
    data['floor'] = 23
    data['max_floor'] = 1
    data['lat'] = 159
    data['log'] = 24
    data['ad_region'] = '77ca3d2e-fcdd-11ee-97ed-0242ac150003'
    data['region'] = '77ca3d2e-fcdd-11ee-97ed-0242ac150003'

    f = open('data/parsed/data.csv', 'a', encoding='utf8')
    f.write(";".join([str(i) for i in data.values()]) + '\n')
    f.close()