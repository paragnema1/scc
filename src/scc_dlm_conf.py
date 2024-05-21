'''
*****************************************************************************
*File : scc_dlm_conf.py
*Module : scc_dlm
*Purpose : SCC configuration class
*Author : Sumankumar Panchal
*Copyright : Copyright 2021, Lab to Market Innovations Private Limited
*****************************************************************************
'''

'''import python packages'''
from json_checker import Checker
import json_checker
from typing import NamedTuple
from os import path
import json
import sys

sys.path.insert(1, "./common")
from scc_log import *
from mqtt_client import *


class SccDlmConfRead:
    schema = {
        "COMMENT": str,
        "VERSION": str,
        "SCC_ID": str,
        "DATABASE": {
            "PROVIDER": str,
            "USER": str,
            "PASSWORD": str,
            "HOST": str,
            "DB_NAME": str
        },
        "LOCAL_MQTT_BROKER": {
            "BROKER_IP_ADDRESS": str,
            "USERNAME": str,
            "PASSWORD": str,
            "PORT": int
        },
        "TOTAL_YARD": int,
        "TOTAL_SECTION": int
    }

    def __init__(self):
        self.comment = None
        self.version = None
        self.database = None
        self.json_data = None
        self.lmb = None
        self.scc_id = None

    def read_cfg(self, file_name):
        if path.exists(file_name): #if file exists then it will load in "self.json_data", if not then it will show error
            with open(file_name) as f:
                try:
                    self.json_data = json.load(f) #loading json data into self.json_data
                    Log.logger.info(
                        f'Configuration File: {file_name} loaded successfully\n {self.json_data}')
                except json.JSONDecodeError as jex:
                    Log.logger.critical(
                        f'{file_name} does not have valid Json Config\n{jex}\n  Program terminated') #jex(error message) is shown if loading failed
                    sys.exit(2) #terminate program after error occuruence
        else:
            Log.logger.critical(f'{file_name} not found.  Program terminated')
            sys.exit(1)
        try: #block to check validation of json data (whether json data is mathcing with schema or not)
            checker = Checker(SccDlmConfRead.schema)
            result = checker.validate(self.json_data)
            Log.logger.info(f'{file_name} Checked OK. Result: {result}')
        except json_checker.core.exceptions.DictCheckerError as err: #if error occur then this except will run
            Log.logger.critical(f'{file_name} is not valid {err}')
            sys.exit(3)
        try: #a block to load data from self.json_data to respective variables
            self.comment = self.json_data['COMMENT']
            self.version = self.json_data['VERSION']
            self.lmb = self.json_data['LOCAL_MQTT_BROKER']
            self.scc_id = self.json_data['SCC_ID']

            self.database = DatabaseStruct(**self.json_data['DATABASE'])
            self.validate_cfg()
            Log.logger.info(
                f'Configuration File: {file_name} Read successfully\n')
        except KeyError as jex: #if error occur in loading data then this except will run
            Log.logger.critical(f'{file_name} do not have the data: {jex}')
            sys.exit(3)

    def validate_cfg(self):
        try: #a block to check if any data is empty or whitespace
            if(not (self.json_data['COMMENT'] and not self.json_data['COMMENT'].isspace())):
                Log.logger.critical(f'Invalid COMMENT')
                sys.exit(4)

            if(not (self.json_data['VERSION'] and not self.json_data['VERSION'].isspace())):
                Log.logger.critical(f'Invalid VERSION')
                sys.exit(4)

            if(not (self.json_data['SCC_ID'] and not self.json_data['SCC_ID'].isspace())):
                Log.logger.critical(f'Invalid SCC_ID')
                sys.exit(4)

            lmb = self.json_data['LOCAL_MQTT_BROKER'] #loading local mqtt broker data from scc.config into lmb

            if(not (lmb['BROKER_IP_ADDRESS'] and not lmb['BROKER_IP_ADDRESS'].isspace())):
                Log.logger.critical(f'Invalid BROKER_IP_ADDRESS')
                sys.exit(4)

            pms_db = self.json_data['DATABASE'] #loading database data from scc.config into pms_db

            if(not (pms_db['PROVIDER'] and not pms_db['PROVIDER'].isspace())):
                Log.logger.critical(f'Invalid DATABASE PROVIDER')
                sys.exit(4)

            if(not (pms_db['USER'] and not pms_db['USER'].isspace())):
                Log.logger.critical(f'Invalid DATABASE USER')
                sys.exit(4)

            if(not (pms_db['PASSWORD'] and not pms_db['PASSWORD'].isspace())):
                Log.logger.critical(f'Invalid DATABASE PASSWORD')
                sys.exit(4)

            if(not (pms_db['HOST'] and not pms_db['HOST'].isspace())):
                Log.logger.critical(f'Invalid DATABASE HOST')
                sys.exit(4)

            if(not (pms_db['DB_NAME'] and not pms_db['DB_NAME'].isspace())):
                Log.logger.critical(f'Invalid DATABASE DB_NAME')
                sys.exit(4)
        except Exception as ex:
            Log.logger.critical(f'scc_dlm_conf: validate_cfg: exception: {ex}')


class DatabaseStruct(NamedTuple):
    PROVIDER: str
    USER: str
    PASSWORD: str
    HOST: str
    DB_NAME: str


if __name__ == "__main__":
    if Log.logger is None:
        my_log = Log()

    cfg = SccDlmConfRead()

    cfg.read_cfg('../config/scc.conf') #passing "scc.config" file to method "read_cfg" to read data from it
    Log.logger.info('******************  In Main Program *******************')
    Log.logger.info(f'DATABASE: {cfg.database} \n')
