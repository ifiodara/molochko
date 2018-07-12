import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime



def drug_data(quote_page):
    #quote_page = 'http://p24.by/goods/filter/?category_vid=2&category_cats=11&category_podcat=79'
    page = requests.get(quote_page)
    soup = BeautifulSoup(page.text,'html.parser')
    if (soup.find('div',class_='navigation top').find('ul',id='yw0',class_='pagination').find_all('li')[4].has_attr('class')) and (soup.find('div',class_='navigation top').find('ul',id='yw0',class_='pagination').find_all('li',class_='disabled') is not None):
        changer = 1
    else:
        changer = 0
    product_columns = soup.find(class_='row items').find_all('div',class_='card-product-column')
    resArr = []
    for product_column in product_columns:
        res = []
        price = product_column.find('span',class_='price').find('span',class_='value red').text
        res.append(price)
        try:
            price_old = product_column.find('span',class_='price-old').find('span',class_='value').text
        except AttributeError:
            price_old = ''
        try:
            price_old_currency = product_column.find('span',class_='price-old').find('span',class_='currency').text
        except:
            price_old_currency = ''
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
    return resArr,changer

#link = http://p24.by/goods/filter/?category_vid=2&category_cats=11&category_podcat=79&CatalogProducts_page=2
linkbase = 'http://p24.by/goods/filter/?category_vid=2&category_cats=11&category_podcat=79'
changer = 0
result = []
for i in range (1,6):
    url = linkbase+'&CatalogProducts_page='+str(i)
    temp_res = []
    temp_res, changer = drug_data(url)
    result.append(temp_res)
    print(changer)
    if changer==1:
        break
    else:
        continue


with open('/home/ilya/dev/projects/molochko/data/result_p24.csv','w') as csv_file:
    writer = csv.writer(csv_file, delimiter='±',quoting=csv.QUOTE_NONE, escapechar='\\')
    for lines in result:
        for line in lines:
            writer.writerow(line)

