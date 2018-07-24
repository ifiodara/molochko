import psycopg2
from psycopg2 import sql
import sys
import configparser
import logging
from datetime import datetime
import os
import csv
import glob
from statistics import mean
import re

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
connect_str = "dbname='{0}' user='{1}' host='{2}' password='{3}'".format(dbname,user,host,password)
re.UNICODE
logging.basicConfig(filename=log_file,format='%(asctime)s : %(levelname)s\t: %(message)s',level=logging.DEBUG)
start_time = datetime.now()
###############################################################################

def connect_to_db(connection_string):
    try:
        conn = psycopg2.connect(connection_string)
        logging.info("Connected to DB")
        return conn
    except Exception as e:
        logging.error("Can't connect to DB")
        logging.error(e)

def insert_data(connection_var):
    try:
        conn = connection_var
        cursor = conn.cursor()
        cursor.execute(sql.SQL(""" select count(*) from {0}.{1} """).format(sql.Identifier(schema), sql.Identifier(table)))
        rows = cursor.fetchall()
        print(len(rows))
        logging.info("Inserted {0} rows".format(len(rows)))
    except Exception as e:
        logging.error("Can't insert data to DB")
        logging.error(e)

def get_known_manufactures(connection_var):
    try:
        conn = connection_var
        cursor = conn.cursor()
        cursor.execute(sql.SQL(""" select distinct name_part, manufacture from {0}.{1} where extract_category = {2}""")
            .format(sql.Identifier(supply_schema), sql.Identifier(manufactures_table),sql.Literal(extract_category)))
        rows = cursor.fetchall()
        logging.info("Got {0} manufactures".format(len(rows)))
        return rows
    except Exception as e:
        logging.error("Can't get manufactures list from DB")
        logging.error(e)
        return None

conne = connect_to_db(connect_str)
known_manufactures_list = get_known_manufactures(conne)
known_manufactures = {}
known_manufactures = dict([i[0],i[1]] for i in known_manufactures_list)

#insert_data(conne)
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

            default_size = None
            for part in separated_description:
                if (re.match((r"\d+кг|\d+л"),part) is not None):
                    default_size = float(re.sub(r"\D",'',part))
                    break
                elif (re.match((r"\d+г|\d+мл"),part) is not None):
                    default_size = float(re.sub(r"\D",'',part))/1000
                    break
                elif (re.match((r"\d+мг"),part) is not None):
                    default_size = float(re.sub(r"\D",'',part))/1000000
                    break
                else:
                    default_size = -1.0
                

            fat_content = None
            for part in separated_description:
                pos = part.rfind('%')
                if (pos >-1):
                    if part[:pos].find('-'):
                        temp_part_splitted = re.sub(r",",".",re.sub(r"%","",part[:pos])).split('-')
                        part_splitted = list(map(float,temp_part_splitted))
                        fat_content = mean(part_splitted)
                    else: 
                        fat_content = part[:pos]
                    break
                else:
                    fat_content = None
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
                    std_price = None

            print(str(category)+'|'+str(raw_price)+'|'+str(std_price)+'|'+str(default_size)+'|'+str(fat_content)+'|'+str(manufacture))
            print(line)
            
    

logging.info('Script {0} started'.format(os.path.basename(__file__)))
end_time = datetime.now()
logging.info('Script {0} finished successfully in {1}'.format(script_name, end_time-start_time))
