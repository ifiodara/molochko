import re
from statistics import mean

def extract_size(text_array):
    default_size = None
    for part in text_array:
        if (re.match((r"\d+кг|\d+л"), part) is not None):
            default_size = float(re.sub(r"\D", '', part))
            break
        elif (re.match((r"\d+г|\d+мл"), part) is not None):
            default_size = float(re.sub(r"\D", '', part))/1000
            break
        elif (re.match((r"\d+мг"), part) is not None):
            default_size = float(re.sub(r"\D", '', part))/1000000
            break
        else:
            default_size = -1.0
    return default_size


def extract_fat_content(text_array):
    fat_content = None
    for part in text_array:
        pos = part.rfind('%')
        if (pos > -1):
            if part[:pos].find('-'):
                temp_part_splitted = re.sub(r",", ".", re.sub(r"%", "", part[:pos])).split('-')
                part_splitted = list(map(float, temp_part_splitted))
                fat_content = mean(part_splitted)
            else: 
                fat_content = float(part[:pos])
                break
        else:
            fat_content = -1.0
            continue
    return fat_content