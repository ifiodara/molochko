import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime


def drug_data():
    quote_page = 'http://p24.by/goods/filter/?category_vid=2&category_cats=11&category_podcat=79'
    page = requests.get(quote_page)

    soup = BeautifulSoup(page.text,'html.parser')
    product_columns = soup.find(class_='row items').find_all('div',class_='card-product-column')
    resArr = []
    for product_column in product_columns:
        res = []
        price = product_column.find('span',class_='price').find('span',class_='value red').text
        res.append(price)
        price_old = product_column.find('span',class_='price-old').find('span',class_='value').text
        price_old_currency = product_column.find('span',class_='price-old').find('span',class_='currency').text
        res.append(price_old)
        res.append(price_old_currency)
        name_elem = product_column.find('h2',class_='title-product').find('a')
        name = name_elem.text
        res.append(name)
        name_link = name_elem.get('href')
        res.append('http://p24.by'+name_link)
        extract_ts = datetime.now()
        res.append(extract_ts)
        resArr.append(res)
    return resArr

result = drug_data()

with open('data/result_p24.csv','w') as csv_file:
    writer = csv.writer(csv_file, delimiter='±',quoting=csv.QUOTE_NONE)
    for line in result:
        writer.writerow(line)


