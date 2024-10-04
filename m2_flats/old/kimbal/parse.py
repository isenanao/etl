from selenium import webdriver
from bs4 import BeautifulSoup
from time import sleep
from selenium.webdriver.chrome.options import Options
from random import randint, choice
import os

def getLinks():
    url = 'https://www.avito.ru/moskva/kvartiry/prodam-ASgBAgICAUSSA8YQ?context=H4sIAAAAAAAA_0q0MrSqLraysFJKK8rPDUhMT1WyLrYyNLNSKk5NLErOcMsvyg3PTElPLVGyrgUEAAD__xf8iH4tAAAA&p=1&s=104'

    with open('user_agent_pc.txt', 'r', encoding='utf8') as f:
        agents = f.readlines()

    options = Options()
    options.add_argument("user-agent=" + choice(agents))
    browser = webdriver.Chrome()

    browser.get(url)

    soup = BeautifulSoup(browser.page_source)

    mylinks = soup.find_all("a", {"itemprop": "url", "class": "styles-module-root-QmppR styles-module-root_noVisited-aFA10"})

    with open('links.txt', 'a', encoding='utf8') as f:
        for i in mylinks:
            f.write('https://www.avito.ru' + i['href'] + '\n')


def parseLinks():
    with open('user_agent_pc.txt', 'r', encoding='utf8') as f:
        agents = f.readlines()

    with open('links.txt', 'r', encoding='utf8') as f:
        pages = f.readlines()

    options = Options()
    options.page_load_strategy = 'eager'

    for page in pages:
        options.add_argument("user-agent=" + choice(agents))
        browser = webdriver.Chrome(options=options)
        try:
            f = open('data.csv', 'a', encoding='utf8')

            browser.get(page)

            sleep(1)

            browser.execute_script("window.scrollBy(0,"+ str(randint(14, 1000)) + ");")
            browser.execute_script("window.scrollBy(0,"+ str(randint(14, 1000)) + ");")

            sleep(randint(5, 10))

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
            f.write(",".join([str(i) for i in flat.values()]) + '\n')
            f.close()
        except Exception as e:
            print('Плохая = ' + page)
        os.unlink('links.txt')
getLinks()