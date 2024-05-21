'''
*****************************************************************************
*File : main.py
*Module : OCC server
*Purpose : Insert Yard Configuration 
*Author : Sumankumar Panchal
*Copyright : Copyright 2021, Lab to Market Innovations Private Limited
*****************************************************************************
'''

from scc_dlm_conf import *
from scc_layout_model import *
from scc_dlm_model import *
from scc_dlm_api import *
from mqtt_client import *
from scc_log import *
import pandas as pd
import sys
import time
from os import path
import json
import os
import threading

'''Import custom packges '''
sys.path.insert(1, "./common")


class SectionConfig:
    '''Section configuration'''

    def __init__(self):
        self.json_data = ""

    def read_cfg(self, file_name):
        try:
            if path.exists(file_name):
                with open(file_name) as f:
                    try:
                        self.json_data = json.load(f)
                        return self.json_data
                    except json.JSONDecodeError as jex:
                        Log.logger.critical(
                            f'{file_name} does not have valid Json Config\n{jex}\n  Program terminated')
            else:
                Log.logger.critical(
                    f'{file_name} not found.  Program terminated')
                sys.exit(1)
        except Exception as e:
            Log.logger.critical(f'main.py: read_cfg: Exception: {e}')

    def print_cfg(self):
        try:
            Log.logger.info(
                f'simulator.conf file loaded successfully\n {json.dumps(self.json_data, indent=4)}')
        except Exception as e:
            Log.logger.critical(f'main.py: read_cfg: Exception: {e}')


if __name__ == '__main__':
    '''Initialise logger'''
    if Log.logger is None:
        my_log = Log()

    '''database'''
    dlm_cfg = SccDlmConfRead()
    dlm_cfg.read_cfg('../config/scc.conf')

    scc_api = SccAPI()
    db_conn = scc_api.connect_database(dlm_cfg)

    '''Read section connection info'''
    section_conn_conf = SectionConfig()
    section_conn_conf_data = section_conn_conf.read_cfg(
        '../config/yard_config.conf')

    yard_data = section_conn_conf_data['YARDS']
    Log.logger.info(yard_data)
    yard_data_len = len(yard_data)

    for yard_idx in range(len(yard_data)):
        for dpu_idx in range(len(yard_data[yard_idx]['DPU'])):
            for section_idx in range(len(yard_data[yard_idx]['DPU'][dpu_idx]['SECTIONS'])):
                yard_id = yard_data[yard_idx]['YARD_ID']
                yard_name = yard_data[yard_idx]['YARD_NAME']

                dpu_id = yard_data[yard_idx]['DPU'][dpu_idx]['DPU_ID']
                dpu_name = yard_data[yard_idx]['DPU'][dpu_idx]['DPU_NAME']

                section_id = yard_data[yard_idx]['DPU'][dpu_idx]['SECTIONS'][section_idx]['SECTION_ID']
                section_name = yard_data[yard_idx]['DPU'][dpu_idx]['SECTIONS'][section_idx]['SECTION_NAME']
               
                dp_id = yard_data[yard_idx]['DPU'][dpu_idx]['SECTIONS'][section_idx]['DPS']
                Log.logger.info(f'dp_id : {dp_id}')

                yard_config_table = YardConfigInfo()
                yard_config_table.yard_id = yard_id
                yard_config_table.yard_name = yard_name
                yard_config_table.dpu_id = dpu_id
                yard_config_table.dpu_name = dpu_name
                yard_config_table.section_id = section_id
                yard_config_table.section_name = section_name
                yard_config_table.dp_id = dp_id

                yard_config_table.save()
