# -*- coding: utf-8 -*-
"""
Created on Sun Sep 18 16:26:09 2022

@author: Dell
"""

import sys
import os
import configparser

# print('file name: {}'.format(sys.argv[0]))

# num_sum = sum(map(int,sys.argv[1:]))

# print('sum of numbers entered: {}'.format(num_sum))

if len(sys.argv) > 2:
    country = sys.argv[1]
    account_name = sys.argv[2]
else:
    print('Invalid arguments provided')

CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))   

def read_config_file(config_file):
    
    try:
        config = configparser.ConfigParser()
        config_file_path = os.path.join(CURRENT_DIR, config_file)
        config.read(config_file_path)
        return config
    except Exception as e:
        print(e)
        return config
    


test_config_file_path = os.path.join(CURRENT_DIR,'./{}_{}_config.cfg'.format(country,account_name))    
config_details = read_config_file(test_config_file_path)  

print(config_details.sections())
print(config_details.options('settings'))
print(config_details.get('settings', 'secret_key'))
