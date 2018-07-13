import requests
import csv
import logging
import os.path
from bs4 import BeautifulSoup
from datetime import datetime
import configparser
import sys

################## PARAMETERS AND VARIABLES SECTION ###########################
#config_file = '/home/ilya/dev/projects/molochko/src/p24_extractor.cfg'
config_file = sys.argv[1]
config = configparser.ConfigParser()
config.read(config_file)
script_name = os.path.basename(__file__)
base_url = config['p24.by']['base_url']
log_file = '{0}{1}.log'.format(config['p24.by']['log_file_path'],script_name)
output_file_path = config['p24.by']['output_file_path']
link_base = base_url + config['p24.by']['base_url_suffix']
logging.basicConfig(filename=log_file,format='%(asctime)s : %(levelname)s\t: %(message)s',level=logging.DEBUG)
start_time = datetime.now()
###############################################################################

def drug_data(quote_page):
    page = requests.get(quote_page)
    soup = BeautifulSoup(page.text,'html.parser')
    try:
        if (soup.find('div',class_='navigation top').find('ul',id='yw0',class_='pagination').find_all('li')[4].has_attr('class')) and (soup.find('div',class_='navigation top').find('ul',id='yw0',class_='pagination').find_all('li',class_='disabled') is not None):
            changer = 1
        else:
            changer = 0
    except AttributeError:
        changer = 1
    try:
        product_columns = soup.find(class_='row items').find_all('div',class_='card-product-column')
    except AttributeError:
        logging.warning('No products found')
    resArr = []
    for product_column in product_columns:
        try:
            price = product_column.find('span',class_='price').find('span',class_='value red').text
        except AttributeError:
            price = ''
        try:
            price_old = product_column.find('span',class_='price-old').find('span',class_='value').text
        except AttributeError:
            price_old = ''
        try:
            price_old_currency = product_column.find('span',class_='price-old').find('span',class_='currency').text
        except AttributeError:
            price_old_currency = ''
        try:
            name_elem = product_column.find('h2',class_='title-product').find('a')
        except AttributeError:
            name_elem = ''
        try:
            name = name_elem.text
        except AttributeError:
            name = ''
        try:
            name_link = name_elem.get('href')
        except AttributeError:
            name_link = ''        
        resArr.append([name, price, price_old, price_old_currency, base_url+name_link])
    return resArr,changer

def retrieve_data_for_urls():
    logging.info('Retrieving data from {0}'.format(base_url))
    changer = 0
    result = []
    i = 1
    while True:
        url = link_base+'&CatalogProducts_page='+str(i)
        temp_res = []
        temp_res, changer = drug_data(url)
        logging.info('{0} objects recieved'.format(len(temp_res)))
        result.append(temp_res)
        i+=1
        if changer==1:
            logging.info('Last page just parsed')
            break
        else:
            continue
    return result

def write_file_output():
    result = retrieve_data_for_urls()
    with open('{0}_{1}.csv'.format(output_file_path,str(datetime.now().strftime('%Y%m%d_%H%M%S'))),'w') as csv_file:
        writer = csv.writer(csv_file, delimiter='±',quoting=csv.QUOTE_NONE, escapechar='\\')
        lines_number = 0
        for lines in result:
            for line in lines:
                writer.writerow(line)
                lines_number+=1
        logging.info('{0} lines has been written'.format(lines_number))

logging.info('Script {0} started'.format(os.path.basename(__file__)))
write_file_output()
end_time = datetime.now()
logging.info('Script {0} finished successfully in {1}'.format(script_name, end_time-start_time))