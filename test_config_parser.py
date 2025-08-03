# -*- coding: utf-8 -*-
"""
Created on Sun Sep 18 16:51:32 2022

@author: Dell
"""

from configparser import ConfigParser

# part 1: create config file

config = ConfigParser() # variable 1

config['settings'] = {
    'debug': 'true',
    'secret_key': 'abc123',
    'log_path': '/my_app/log'
}

config['db'] = {
    'db_name': 'myapp_dev',
    'db_host': 'localhost',
    'db_port': '8889'
}

config['files'] = {
    'use_cdn': 'false',
    'images_path': '/my_app/images'
}

with open('./test_dev_config.cfg', 'w') as f:
      config.write(f)
      
with open('./test_dev_config.cfg', 'r') as f:
      print(f.read())
      
# part 2: read config file
      
parser = ConfigParser() # variable 2
parser.read('test_dev_config.cfg') # activating variable 2 to read config file

print(parser.sections())
print(parser.options('settings'))
print(parser.get('settings', 'secret_key'))

print(parser.get('db', 'db_port'),type(parser.get('db', 'db_port')))
print(int(parser.get('db', 'db_port')))
print(parser.getint('db', 'db_port'))

print(parser.getint('db', 'db_default_port', fallback=3306))
