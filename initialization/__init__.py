# defining global variabel
import os
import json

basePath = os.path.dirname(os.path.abspath(__file__))
config_file_path = basePath + "/config.json"
with open(config_file_path, 'r') as datafile:
    CONFIG = json.load(datafile)
print("initialize DataLoader package")
