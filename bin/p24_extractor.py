import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime



def drug_data(quote_page):
    page = requests.get(quote_page)
    soup = BeautifulSoup(page.text,'html.parser')
    if (soup.find('div',class_='navigation top').find('ul',id='yw0',class_='pagination').find_all('li')[4].has_attr('class')) and (soup.find('div',class_='navigation top').find('ul',id='yw0',class_='pagination').find_all('li',class_='disabled') is not None):
        changer = 1
    else:
        changer = 0
    product_columns = soup.find(class_='row items').find_all('div',class_='card-product-column')
    resArr = []
    for product_column in product_columns:
        price = product_column.find('span',class_='price').find('span',class_='value red').text
        try:
            price_old = product_column.find('span',class_='price-old').find('span',class_='value').text
        except AttributeError:
            price_old = ''
        try:
            price_old_currency = product_column.find('span',class_='price-old').find('span',class_='currency').text
        except:
            price_old_currency = ''
        name_elem = product_column.find('h2',class_='title-product').find('a')
        name = name_elem.text
        name_link = name_elem.get('href')
        resArr.append([name, price, price_old, price_old_currency, name_link])
    return resArr,changer

linkbase = 'http://p24.by/goods/filter/?category_vid=2&category_cats=11&category_podcat=79'
changer = 0
result = []
i = 1
while True:
    url = linkbase+'&CatalogProducts_page='+str(i)
    temp_res = []
    temp_res, changer = drug_data(url)
    result.append(temp_res)
    i+=1
    if changer==1:
        break
    else:
        continue

with open('/home/ilya/dev/projects/molochko/data/p24_{0}.csv'.format(str(datetime.now().strftime('%Y%m%d_%H%M%S'))),'w') as csv_file:
    writer = csv.writer(csv_file, delimiter='±',quoting=csv.QUOTE_NONE, escapechar='\\')
    for lines in result:
        for line in lines:
            writer.writerow(line)

