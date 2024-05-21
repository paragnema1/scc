'''
*****************************************************************************
*File : main.py
*Module : OCC server
*Purpose : Section evaluation
*Author : Sumankumar Panchal
*Copyright : Copyright 2020, Lab to Market Innovations Private Limited
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


if __name__ == '__main__': #execute if code is being run directly by python interpreter
    '''Initialise logger'''
    if Log.logger is None:
        my_log = Log()

    section_conf = SectionConfig()
    cfg = section_conf.read_cfg('../config/section.conf') #read section configuration file

    '''database'''
    dlm_cfg = SccDlmConfRead()
    dlm_cfg.read_cfg('../config/scc.conf') #read database configuration file

    scc_api = SccAPI()
    db_conn = scc_api.connect_database(dlm_cfg) #connect to database 'dlm_cfg'

    '''Fill section information'''
    section_record_len = len(cfg['SECTION']) #get number of sections

    scc_config_table = SectionConfigInfo()

    qry = scc_config_table.delete() 
    qry.execute() #delete all prexisting records from section configuration table (but why?)

    for i in range(section_record_len): #itering through each detection point of each section
        section_id = cfg['SECTION'][i]['SECTION_ID']
        for j in range(len(cfg['SECTION'][i]['SECTION_IN'])): #SECTION IN iteration of each data point
            section_type = "SECTION_IN"
            dp_id = cfg['SECTION'][i]['SECTION_IN'][j]['DP_ID']
            dp_direction = cfg['SECTION'][i]['SECTION_IN'][j]['DP_DIRECTION']

            scc_config_table = SectionConfigInfo()
            scc_config_table.section_id = section_id
            scc_config_table.section_type = section_type
            scc_config_table.dp_id = dp_id
            scc_config_table.dp_direction = dp_direction
            scc_config_table.save()

        for j in range(len(cfg['SECTION'][i]['SECTION_OUT'])): #SECTION OUT iteration of each data point
            section_type = "SECTION_OUT"
            dp_id = cfg['SECTION'][i]['SECTION_OUT'][j]['DP_ID']
            dp_direction = cfg['SECTION'][i]['SECTION_OUT'][j]['DP_DIRECTION']

            scc_config_table = SectionConfigInfo()
            scc_config_table.section_id = section_id
            scc_config_table.section_type = section_type
            scc_config_table.dp_id = dp_id
            scc_config_table.dp_direction = dp_direction
            scc_config_table.save()

    '''Read section connection info
    section_conn_conf = SectionConfig()
    section_conn_conf_data = section_conn_conf.read_cfg(
        '../config/section_connections.conf')
    section_conn_conf_data_len = len(section_conn_conf_data["SECTIONS"])

    for data_idx in range(section_conn_conf_data_len):
        section_id = section_conn_conf_data["SECTIONS"][data_idx]["SECTION_ID"]
        left_normal = section_conn_conf_data["SECTIONS"][data_idx]["LEFT_NORMAL"]
        right_normal = section_conn_conf_data["SECTIONS"][data_idx]["RIGHT_NORMAL"]
        left_reverse = section_conn_conf_data["SECTIONS"][data_idx]["LEFT_REVERSE"]
        right_reverse = section_conn_conf_data["SECTIONS"][data_idx]["RIGHT_REVERSE"]

        section_conn_table = LayoutSectionConnectionsInfo()
        section_conn_table.section_id = section_id
        section_conn_table.left_normal = left_normal
        section_conn_table.right_normal = right_normal
        section_conn_table.left_reverse = left_reverse
        section_conn_table.right_reverse = right_reverse

        section_conn_table.save()
    '''
