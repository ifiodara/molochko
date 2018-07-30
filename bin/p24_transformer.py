import psycopg2
from psycopg2 import sql
import sys
import configparser
import logging
from datetime import datetime
import os
import csv
import glob
import re
import common.fields_conversion as cfc
import common.db as cdb
from statistics import mean

################## PARAMETERS AND VARIABLES SECTION ###########################
#config_file = '/home/ilya/dev/projects/molochko/src/p24_transformer.cfg'
config_file = sys.argv[1]
config = configparser.ConfigParser()
config.read(config_file)
script_name = os.path.basename(__file__)
log_file = '{0}{1}.log'.format(config['p24.by']['log_file_path'],script_name)
input_file_path = config['p24.by']['input_file_path']
dbname = config['dbconf']['dbname']
user = config['dbconf']['user']
host = config['dbconf']['host']
password = config['dbconf']['password']
table = config['dbconf']['table']
schema = config['dbconf']['schema']
manufactures_table =config['dbconf']['manufactures_table']
supply_schema = config['dbconf']['supply_schema']
extract_category = 'молоко'
result_column_list = config['dbconf']['result_column_list']
connect_str = "dbname='{0}' user='{1}' host='{2}' password='{3}'".format(dbname,user,host,password)
re.UNICODE
logging.basicConfig(filename=log_file,format='%(asctime)s : %(levelname)s\t: %(message)s',level=logging.DEBUG)
start_time = datetime.now()
###############################################################################

# TO DO
# 1. rename input file on success               5
# 2. cleanup code                               3
# 3. create table for product abbreviations     2
# 4. insert processed data to db                1
# 5. create data ouput objec/class              4
# 6. put config for db to separate file         6

final_result = []

conne = cdb.connect_to_db(connect_str)


known_manufactures_list = cdb.get_known_manufactures(conne, supply_schema, manufactures_table, extract_category)
known_manufactures = {}
known_manufactures = dict([i[0],i[1]] for i in known_manufactures_list)


output_data = []
input_files = glob.glob("{0}p24*.csv".format(input_file_path))
for input_file in input_files:
    print(input_file)
    with open(input_file) as input_data_file:
        input_data = csv.reader(input_data_file, delimiter='±',quoting=csv.QUOTE_NONE, escapechar='\\')
        i=0
        for line in input_data:
            raw_category = line[0]
            if (raw_category.lower().find('сгущ')>-1):
                category = 'сгущённое молоко'
            elif(raw_category.lower().find('слив')>-1) or (raw_category.lower().find('крем')>-1):
                category = 'сливки'
            elif (raw_category.lower().find('кокт')>-1):
                category = 'коктейль'
            elif (raw_category.lower().find('йог')>-1):
                category = 'йогурт'
            elif (raw_category.lower().find('молоко')>-1):
                category = 'молоко'
            else:
                category = None
            description = line[0].strip()

            raw_price = line[1].strip()
            if (raw_price is not None) and (raw_price != ''):
                raw_price = float(raw_price)
            else: 
                raw_price = -1.0

            separated_description = raw_category.split(' ')

            default_size = cfc.extract_size(separated_description)
                

            #fat_content = cfc.extract_fat_content(separated_description)
            for part in separated_description:
                pos = part.rfind('%')
                if (pos >-1):
                    if part[:pos].find('-'):
                        temp_part_splitted = re.sub(r",",".",re.sub(r"%","",part[:pos])).split('-')
                        part_splitted = list(map(float,temp_part_splitted))
                        fat_content = mean(part_splitted)
                    else: 
                        fat_content = float(part[:pos])
                    break
                else:
                    fat_content = -1.0
                    continue

            manufacture = None
            for k, v in known_manufactures.items():
                if (raw_category.lower().find(k.lower())>-1):
                    manufacture = v

            std_price = line[2].strip()
            if (std_price is None) or (std_price.strip() == ''):
                if (raw_price is not None) and (raw_price > -1.0) and (default_size is not None) and (default_size > -1.0):
                    std_price = float(raw_price/default_size)
                else:
                    std_price = -1.0
            else:
                std_price = float(std_price)

            description = description
            category = category
            raw_price = round(raw_price,2)
            std_price = round(std_price,2)
            default_size = round(default_size,2)
            fat_content = round(fat_content,2)
            manufacture = manufacture
            
            final_result.append((category,description,raw_price,std_price,default_size,fat_content, manufacture))

# print(final_result)
cdb.batch_insert_data(conne,final_result,schema,table,result_column_list)
             

logging.info('Script {0} started'.format(os.path.basename(__file__)))
end_time = datetime.now()
logging.info('Script {0} finished successfully in {1}'.format(script_name, end_time-start_time))
