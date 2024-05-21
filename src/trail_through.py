'''
*****************************************************************************
*File : scc_trail_through.py
*Module : SCC 
*Purpose : SCC trail through detection 
*Author : Sumankumar Panchal
*Copyright : Copyright 2021, Lab to Market Innovations Private Limited
*****************************************************************************
'''

'''Import packages '''
from scc_dlm_conf import *
from scc_log import *
import sys
import json
import os

from peewee import *
from datetime import datetime, timedelta
from scc_dlm_model import *
from scc_layout_model import *
from scc_dlm_api import *
sys.path.insert(1, "./common")

class Sec:
    def __init__(self):
        self.section_id = "none"
        self.left_normal = "none"
        self.right_normal = "none"
        self.left_reverse = "none"
        self.right_reverse = "none"
        #self.engine_axle_count = 0
        self.torpedo_axle_count = 0
        self.section_status = "none"
        self.point_id = "none"
        self.point_status = "none"
        self.point_mode = "none"
        self.point_error = 0
        #self.torpedo_status = "none"
        self.direction = "none"
        #self.speed = 10
        #self.first_axle = "none"
        self.error_code = 0

class Trailthrough:
    def __init__(self, mqtt_client):
        self.scc_api = SccAPI()
        self.db_conn = self.scc_api.connect_database(cfg)
        self.tt_sec_obj_list = []
        self.total_pm_sec = 0
        self.mqtt_client = mqtt_client
        self.prev_sections_info = {}

    def get_point_config(self):
        '''get pms configuration from database table'''
        try:
            point_config_record = PointConfig.select()
            return point_config_record
        except Exception as ex:
            Log.logger.critical(f'get_point_config: exception: {ex}')

    def init_trail_through_info(self):
        '''init section connections information'''
        try:

            Log.logger.info(f'init trail through info called')
            if self.db_conn:
                section_connections_db_records = self.scc_api.read_section_connections_info()

                for sc in section_connections_db_records:
                    Log.logger.info(
                        f'SECTION_ID: {sc.section_id}, LEFT_SECTION: {sc.left_normal}, RIGHT_SECTION: {sc.right_normal}')

                for sc in section_connections_db_records:
                    self.tt_sec_obj_list.append(Sec())

                sc_idx = 0
                for sc in section_connections_db_records:
                    self.tt_sec_obj_list[sc_idx].section_id = sc.section_id
                    self.tt_sec_obj_list[sc_idx].left_normal = sc.left_normal
                    self.tt_sec_obj_list[sc_idx].right_normal = sc.right_normal
                    self.tt_sec_obj_list[sc_idx].left_reverse = sc.left_reverse
                    self.tt_sec_obj_list[sc_idx].right_reverse = sc.right_reverse
                    sc_idx += 1

                point_config = self.get_point_config()

                for point in point_config:
                    for sec_idx in range(len(self.tt_sec_obj_list)):
                        if self.tt_sec_obj_list[sec_idx].section_id == point.section_id:
                            self.tt_sec_obj_list[sec_idx].point_id = point.point_id

                            Log.logger.info(
                                f'SECTION_ID: {self.tt_sec_obj_list[sec_idx].section_id}, POINT_ID: {self.tt_sec_obj_list[sec_idx].point_id}')
                        else:
                            pass
                self.total_pm_sec = len(self.tt_sec_obj_list)
            else:
                pass
                Log.logger.warning(f'database not connected!!')

        except Exception as ex:
            Log.logger.critical(
                f'init_section_connections_info: exception {ex}')
    '''trail through detection using section status and point status'''
    def detect_trail_through(self, section_json_data, point_data):        
        try:
            sections_info = {}
            sections_data = json.loads(section_json_data)

            for json_idx in range(len(sections_data['sections'])):
                sections_info[sections_data['sections'][json_idx]
                             ['section_id']] = sections_data['sections'][json_idx]
                
            '''update point status and point mode'''
            for point_idx in range(len(point_data)):
                for sec_idx in range(self.total_pm_sec):
                    if self.tt_sec_obj_list[sec_idx].point_id == point_data[point_idx].point_id:
                        self.tt_sec_obj_list[sec_idx].point_status = point_data[point_idx].point_status
                        self.tt_sec_obj_list[sec_idx].point_mode = point_data[point_idx].point_mode
            
            '''update section status having PM'''
            for pm_sec_idx in range(self.total_pm_sec):
                sec_idx = int(self.tt_sec_obj_list[pm_sec_idx].section_id.replace('S','')) - 1
                if self.tt_sec_obj_list[pm_sec_idx].section_id == sections_data['sections'][sec_idx]['section_id']:
                    self.tt_sec_obj_list[pm_sec_idx].section_status = sections_data['sections'][sec_idx]['section_status']
                    self.tt_sec_obj_list[pm_sec_idx].direction = sections_data['sections'][sec_idx]['direction']
                    self.tt_sec_obj_list[pm_sec_idx].torpedo_axle_count = sections_data['sections'][sec_idx]['torpedo_axle_count']
                    self.tt_sec_obj_list[pm_sec_idx].error_code = sections_data['sections'][sec_idx]['error_code']
             
            tt_section_id = []
            
            for pm_sec_idx in range(self.total_pm_sec):  
     
                right_normal_sec_id = self.tt_sec_obj_list[pm_sec_idx].right_normal
                left_normal_sec_id = self.tt_sec_obj_list[pm_sec_idx].left_normal
                right_reverse_sec_id = self.tt_sec_obj_list[pm_sec_idx].right_reverse
                left_reverse_sec_id = self.tt_sec_obj_list[pm_sec_idx].left_reverse

                trail_through_flag = False
                if self.tt_sec_obj_list[pm_sec_idx].section_status == "occupied" and\
                        self.tt_sec_obj_list[pm_sec_idx].point_mode != "manual"\
                        and len(self.prev_sections_info)!= 0:
                    if left_normal_sec_id != '' and left_reverse_sec_id != '':
                        if self.tt_sec_obj_list[pm_sec_idx].direction == "in" and\
                            self.tt_sec_obj_list[pm_sec_idx].point_status == "reverse":
                            if sections_info[left_normal_sec_id]["section_status"] == "occupied" and\
                                sections_info[left_normal_sec_id]["direction"] == "in" and\
                                sections_info[left_normal_sec_id]["torpedo_axle_count"] != self.prev_sections_info[left_normal_sec_id]["torpedo_axle_count"]:
                                    trail_through_flag = True
                        elif self.tt_sec_obj_list[pm_sec_idx].direction == "in" and\
                            self.tt_sec_obj_list[pm_sec_idx].point_status == "normal":
                            if sections_info[left_reverse_sec_id]["section_status"] == "occupied" and\
                                sections_info[left_reverse_sec_id]["direction"] == "in" and\
                                sections_info[left_reverse_sec_id]["torpedo_axle_count"] != self.prev_sections_info[left_reverse_sec_id]["torpedo_axle_count"]:
                                    trail_through_flag = True
                    elif right_normal_sec_id != '' and right_reverse_sec_id != '':
                        if self.tt_sec_obj_list[pm_sec_idx].direction == "out" and\
                            self.tt_sec_obj_list[pm_sec_idx].point_status == "reverse":
                            if sections_info[right_normal_sec_id]["section_status"] == "occupied" and\
                                sections_info[right_normal_sec_id]["direction"] == "out" and\
                                sections_info[right_normal_sec_id]["torpedo_axle_count"] != self.prev_sections_info[right_normal_sec_id]["torpedo_axle_count"]:
                                   trail_through_flag = True    
                        elif self.tt_sec_obj_list[pm_sec_idx].direction == "out" and\
                            self.tt_sec_obj_list[pm_sec_idx].point_status == "normal":
                            if sections_info[right_reverse_sec_id]["section_status"] == "occupied" and\
                                sections_info[right_reverse_sec_id]["direction"] == "out" and\
                                sections_info[right_reverse_sec_id]["torpedo_axle_count"] != self.prev_sections_info[right_reverse_sec_id]["torpedo_axle_count"]:
                                   trail_through_flag = True   

                if(trail_through_flag):
                    Log.logger.info(f'trail-through detected in Section id:{self.tt_sec_obj_list[pm_sec_idx].section_id}')
                    tt_section_id.append(self.tt_sec_obj_list[pm_sec_idx].section_id)                               

            Log.logger.info(f'return value: {tt_section_id}')
            self.prev_sections_info = sections_info
            return tt_section_id
        except Exception as ex:
            Log.logger.critical(f'find trail through: exception: {ex}')


