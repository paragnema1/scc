'''
*****************************************************************************
*File : main.py
*Module : SCC server
*Purpose : Section evaluation
*Author : Sumankumar Panchal
*Copyright : Copyright 2021, Lab to Market Innovations Private Limited
*****************************************************************************
'''

from scc_dlm_conf import *
from scc_dlm_model import *
from scc_dlm_api import *
from common.mqtt_client import *
from common.scc_log import *
#from scc_trail_through import *
from trail_through import *

import pandas as pd
import sys
import time
from os import path
import json
import os
import threading
import signal

'''Import custom packges '''
sys.path.insert(1, "./common")

TOTAL_YARD = 1
TOTAL_SECTION = 1
TOTAL_DP = 3


class Point:
    def __init__(self):
        self.point_id = "none"
        self.point_status = "none"
        self.point_mode = "none"
        self.error_code = "100"
        self.section_id = "none"
        self.point_status_request = "none"
        self.last_pms_send_msg_ts = 0.0
        self.last_pmm_recv_msg_ts = 0.0
        self.username = "none"
        self.point_ip_address = "0.0.0.0"


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


class Yard:
    def __init__(self):
        self.yard_id = 0
        self.yard_name = "none"
        self.dpu_id = "none"
        self.dpu_name = "none"
        self.section_id = "none"
        self.section_name = "none"


class Section:
    def __init__(self):
        self.yard_id = 0
        self.yard_name = "none"
        self.dpu_id = "none"
        self.dpu_name = "none"
        self.section_id = "none"
        self.section_name = "none"
        self.section_status = "cleared"
        self.direction = "none"
        self.speed = 10
        self.engine_axle_count = 0
        self.torpedo_axle_count = 0
        self.torpedo_status = "loaded"
        self.first_axle = "torpedo"
        self.left_normal_section_id = "none"
        self.right_normal_section_id = "none"
        self.left_reverse_section_id = "none"
        self.right_reverse_section_id = "none"
        self.torpedo_id = 0
        self.engine_id = 0
        self.prev_axle_count = 0
        self.error_code = 0
        self.my_idx = 0
        self.left_normal_section_status = "none"
        self.right_normal_section_status = "none"
        self.left_reverse_section_status = "none"
        self.right_reverse_section_status = "none"
        self.dp_id = []

class Sccserver:
    def __init__(self, mqtt_client):
        try:
            self.scc_api = SccAPI()
            
            self.scc_tt = Trailthrough(mqtt_client)
            self.scc_tt.init_trail_through_info()

            self.mqtt_client = mqtt_client
            Log.logger.info("SCC Server initialised!!")

            self.yard_obj_list = []
            self.section_obj_list = []
            self.point_obj_list = []
        except Exception as ex:
            Log.logger.critical(f'init exception: {ex}')

    def init_section_info(self):
        for section_idx in range(TOTAL_SECTION):
            self.section_obj_list.append(Section())

        for section_idx in range(TOTAL_SECTION):
            self.section_obj_list[section_idx].section_id = "S" + \
                str(section_idx + 1)

    def cwsm_section_reset_sub_fn(self, in_client, user_data, message):
        '''cwsm section reset subscribe function'''
        try:
            json_section_reset_msg = json.loads(message.payload)
            Log.logger.info(json_section_reset_msg)

            if self.check_user_roles(json_section_reset_msg["username"]):
                self.section_reset_pub_fn(json_section_reset_msg)
                event_ts = time.time()
                event_id = "SCC-EVENT-01"
                event_desc = "section " + \
                    json_section_reset_msg["section_id"] + " reset performed."

                '''insert section reset event in the database'''
                self.scc_api.insert_event_info(event_ts, event_id, event_desc)
            else:
                pass
        except Exception as ex:
            Log.logger.critical(f'cwsm_section_reset_sub_fn: exception: {ex}')

    def cwsm_dp_reset_sub_fn(self, in_client, user_data, message):
        '''cwsm dp reset subscribe function'''
        try:
            json_dp_reset_msg = json.loads(message.payload)
            Log.logger.info(json_dp_reset_msg)

            if self.check_user_roles(json_dp_reset_msg["username"]):
                self.dp_reset_pub_fn(json_dp_reset_msg)
                event_ts = time.time()
                event_id = "SCC-EVENT-02"
                event_desc = "dp " + \
                    json_dp_reset_msg["dp_id"] + " reset performed."

                '''insert section reset event in the database'''
                self.scc_api.insert_event_info(event_ts, event_id, event_desc)
            else:
                pass
        except Exception as ex:
            Log.logger.critical(f'cwsm_dp_reset_sub_fn: exception: {ex}')

    def check_user_roles(self, username):
        '''check user role from database'''
        try:
            user_permission = False
            user_roles = self.scc_api.get_user_roles(username)

            if user_roles is not None:
                if len(user_roles) != 0:
                    for role_idx in range(len(user_roles)):
                        if user_roles[role_idx] == "Command Center Admin" or user_roles[role_idx] == "Admin":
                            user_permission = True
                        else:
                            user_permission = False
                else:
                    pass
            else:
                pass

            return user_permission
        except Exception as ex:
            Log.logger.critical(f'check_user_roles: exception: {ex}')

    def section_reset_pub_fn(self, json_section_reset_msg_param):
        '''publish section reset message to acp dpu'''
        try:
            section_dp_id_list = self.get_dp_list_of_section(json_section_reset_msg_param["section_id"])
            section_reset_msg = {
                    "ts": time.time(), "username": json_section_reset_msg_param["username"], "section_id": json_section_reset_msg_param["section_id"], "section_name": json_section_reset_msg_param["section_name"], "dp_id": section_dp_id_list}
            json_section_reset_msg = json.dumps(section_reset_msg)
            dpu_id = self.scc_api.get_dpu_id(
                json_section_reset_msg_param["section_id"])
            mqtt_topic_1 = ("occ/section_reset").lower()
            mqtt_topic_2 = ("scc/section_reset").lower()

            self.mqtt_client.pub(mqtt_topic_1, json_section_reset_msg)
            self.mqtt_client.pub(mqtt_topic_2, json_section_reset_msg)
        except Exception as ex:
            Log.logger.critical(f'section_reset_pub_fn: exception: {ex}')

    def get_dp_list_of_section(self, section_id):
        try:
            for sec_idx in range(len(self.section_obj_list)):
                if self.section_obj_list[sec_idx].section_id == section_id:
                    return self.section_obj_list[sec_idx].dp_id
                else:
                    pass
            return []
        except Exception as ex:
            Log.logger.critical(f'get_dp_list_of_section: exception: {ex}')

    def dp_reset_pub_fn(self, json_dp_reset_msg_param):
        '''publish dp reset message to acp dpu'''
        try:
            dp_reset_msg = {"dp_id": json_dp_reset_msg_param["dp_id"], "in_count": -1, "out_count": -1}
            json_dp_reset_msg = json.dumps(dp_reset_msg)
            mqtt_topic = ("scc/dp_reset").lower()

            self.mqtt_client.pub(mqtt_topic, json_dp_reset_msg)
        except Exception as ex:
            Log.logger.critical(f'dp_reset_pub_fn: exception: {ex}')

    def fill_yard_config_info_from_db(self):
        ''' Fill yard configuration information from yard_config db table'''
        try:
            db_yard_config_list = self.scc_api.read_yard_config_info()

            if len(db_yard_config_list) > 0:
                for section_idx in db_yard_config_list:
                    Log.logger.info(
                        f'{section_idx.yard_id}, {section_idx.yard_name},'
                        f'{section_idx.dpu_id}, {section_idx.dpu_name},'
                        f'{section_idx.section_id}, {section_idx.section_name}, {section_idx.dp_id}')

                    new_section_config_obj = Section()
                    new_section_config_obj.yard_id = section_idx.yard_id
                    new_section_config_obj.yard_name = section_idx.yard_name
                    new_section_config_obj.dpu_id = section_idx.dpu_id
                    new_section_config_obj.dpu_name = section_idx.dpu_name
                    new_section_config_obj.section_id = section_idx.section_id
                    new_section_config_obj.section_name = section_idx.section_name
                    new_section_config_obj.dp_id = section_idx.dp_id

                    self.section_obj_list.append(new_section_config_obj)

            else:
                pass
                Log.logger.warning(f'yard configuration table found empty')
                '''stop application if table is empty'''
                sys.exit(0)
        except Exception as ex:
            Log.logger.critical(
                f'fill_yard_config_info_from_db: exception: {ex}')

    def fill_section_connections_info_from_db(self):
        try:
            db_section_connections_list = self.scc_api.read_section_connections_info()

            if len(db_section_connections_list) > 0:
                for db_section_idx in db_section_connections_list:
                    Log.logger.info(
                        f'{db_section_idx.section_id}, {db_section_idx.left_normal}, {db_section_idx.right_normal}, {db_section_idx.left_reverse}, {db_section_idx.right_reverse}')

                    for section_obj_idx in range(len(self.section_obj_list)):
                        if self.section_obj_list[section_obj_idx].section_id == db_section_idx.section_id:
                            self.section_obj_list[section_obj_idx].left_normal_section_id = db_section_idx.left_normal
                            self.section_obj_list[section_obj_idx].right_normal_section_id = db_section_idx.right_normal
                            self.section_obj_list[section_obj_idx].left_reverse_section_id = db_section_idx.left_reverse
                            self.section_obj_list[section_obj_idx].right_reverse_section_id = db_section_idx.right_reverse
                        else:
                            pass
            else:
                pass
                Log.logger.warning(f'section connection table found empty')
                '''stop application if table is empty'''
                sys.exit(0)
        except Exception as ex:
            Log.logger.critical(
                f'fill_section_connections_info_from_db: exception: {ex}')

    def get_section_status(self, section_id):
        try:
            for section_obj_idx in range(len(self.section_obj_list)):
                if self.section_obj_list[section_obj_idx].section_id == section_id:
                    return self.section_obj_list[section_obj_idx].section_status
                else:
                    pass
            return "none"
        except Exception as ex:
            Log.logger.critical("fill section idx: exception: {ex}")

    def print_section_info(self):
        try:
            for section_idx in range(len(self.section_obj_list)):
                Log.logger.info(f'section id:{self.section_obj_list[section_idx].section_id},'
                                f'section_status:{self.section_obj_list[section_idx].section_status},'
                                f'torpedo_axle_count:{self.section_obj_list[section_idx].torpedo_axle_count},'
                                f'engine_axle_count:{self.section_obj_list[section_idx].engine_axle_count},'
                                f'direction: {self.section_obj_list[section_idx].direction},'
                                f'speed: {self.section_obj_list[section_idx].speed},'
                                f'torpedo_status: {self.section_obj_list[section_idx].torpedo_status},'
                                f'first_axle: {self.section_obj_list[section_idx].first_axle}')
        except Exception as ex:
            Log.logger.info(f'print_section_info: exception: {ex}')

    def construct_section_json_msg(self):
        try:
            section_msg_list = []
            json_scc_msg = ""
            for i in range(len(self.section_obj_list)):
                section_msg = {
                    "section_id": self.section_obj_list[i].section_id,
                    "section_status": self.section_obj_list[i].section_status,
                    "engine_axle_count": self.section_obj_list[i].engine_axle_count,
                    "torpedo_axle_count": self.section_obj_list[i].torpedo_axle_count,
                    "direction": self.section_obj_list[i].direction,
                    "speed": self.section_obj_list[i].speed,
                    "torpedo_status": self.section_obj_list[i].torpedo_status,
                    "first_axle": self.section_obj_list[i].first_axle,
                    "error_code": self.section_obj_list[i].error_code}
                section_msg_list.append(section_msg)

            scc_msg = {"ts": time.time(), "sections": section_msg_list}
            json_scc_msg = json.dumps(scc_msg, indent=0)
            return json_scc_msg
        except Exception as e:
            Log.logger.critical(f'construct_section_json_msg: exception : {e}')

    def publish_section_info(self, mqtt_client, scc_api):
        try:
            while True:
                json_msg = self.construct_section_json_msg()
                ''' insert section_info in database first before publish'''
                scc_api.insert_section_info(json_msg)
                scc_api.insert_section_playback_info(json_msg)
                # dlm_api.insert_train_trace_info(json_msg)
                # dlm_api.yard_performance(json_msg)

                ''' publish section_info '''
                mqtt_client.pub("occ/section_info", json_msg)
                time.sleep(1)
        except Exception as e:
            Log.logger.critical(f'publish_section_info: exception: {e}')

    def evaluator_section_info_sub_fn(self, in_client, user_data, message):
        '''subscribe sem/section_info receive from acp dpu'''
        try:
            ts_start = time.time()

            Log.logger.info(f'sem/section_info received time: {ts_start}')
            recv_msg = json.loads(message.payload)
            recv_json_msg = json.dumps(recv_msg, indent=0)
            
            json_msg = recv_json_msg
            '''get torpedo status of middle sections'''
            #json_msg = self.scc_tt.find_torpedo_status(recv_json_msg)  #NOT REQUIRED IN HSM1 SCENARIO

            ''' insert section_info in database first before publish'''
            scc_api.insert_section_info(json_msg)
            scc_api.insert_section_playback_info(json_msg)
            scc_api.insert_train_trace_info(json_msg)
            #scc_api.yard_performance(json_msg)
            #scc_api.torpedo_performance(json_msg)

            '''trail through early warning detection'''
            tt_sec_list = self.scc_tt.detect_trail_through(
                json_msg, self.point_obj_list)

            if len(tt_sec_list) != 0:
                for sec_idx in range(len(tt_sec_list)):
                    tt_msg = json.dumps(
                        {"ts": time.time(), "section_id": (tt_sec_list[sec_idx]).lower()})
                    mqtt_client.pub("scc/trail_through", tt_msg)
            else:
                pass

            ''' publish section_info '''
            mqtt_client.pub("occ/section_info", json_msg)

            ts_end = time.time()
            total_ts = ts_end - ts_start
            Log.logger.info(f'scc evaluator function execution time {total_ts}')

        except Exception as ex:
            Log.logger.critical(f'evaluator_section_info_sub_fn: exception: {ex}')

    def load_point_config(self):
        '''load point configuration from pms_config table'''
        try:
            point_config_obj = self.scc_tt.get_point_config()

            for point_idx in point_config_obj:
                self.point_obj_list.append(Point())
                self.point_obj_list[-1].point_id = point_idx.point_id
                self.point_obj_list[-1].section_id = point_idx.section_id

            for point_idx in range(len(self.point_obj_list)):
                Log.logger.info(
                    f'point_id: {self.point_obj_list[point_idx].point_id}, section_id: {self.point_obj_list[point_idx].section_id}')

        except Exception as ex:
            Log.logger.critical(f'load_point_status: exception {ex}')

    def point_info_sub_fn(self, in_client, user_data, message):
        '''subscribe pms/point_info receive from pms'''
        try:
            msg_payload = json.loads(message.payload)
            #Log.logger.info(f'Point Info : {message.payload}')
            for point_idx in range(len(self.point_obj_list)):
                if self.point_obj_list[point_idx].point_id == msg_payload["point_id"]:
                    self.point_obj_list[point_idx].point_status = msg_payload["point_status"]
                    self.point_obj_list[point_idx].point_mode = msg_payload["point_mode"]
                    self.point_obj_list[point_idx].error_code = msg_payload["error_code"]
                    self.point_obj_list[point_idx].ts = msg_payload["ts"]
                else:
                    pass
            # self.print_point_info()
        except Exception as ex:
            Log.logger.critical(f'point_info_sub_fn: exception: {ex}')

    def print_point_info(self):
        try:
            for point_idx in range(len(self.point_obj_list)):
                Log.logger.info(
                    f'{self.point_obj_list[point_idx].ts},'
                    f'{self.point_obj_list[point_idx].point_id},'
                    f'{self.point_obj_list[point_idx].point_status},'
                    f'{self.point_obj_list[point_idx].point_mode}')
        except Exception as ex:
            Log.logger.critical(f'print_point_info: exception: {ex}')

    def tt_info_sub_fn(self, in_client, user_data, message):
        '''subscribe scc/trail_through receive from SCC'''
        try:
            tt_msg_payload = json.loads(message.payload)
            self.scc_api.insert_trail_through_info(tt_msg_payload)
        except Exception as ex:
            Log.logger.critical(f'tt_info_sub_fn: exception: {ex}')

    def tt_clear_sub_fn(self, in_client, user_data, message):
        '''subscribe cwsm/tt_clear receive from UI'''
        try:
            tt_clear_msg_payload = json.loads(message.payload)
            self.scc_api.clear_trail_through(tt_clear_msg_payload)
        except Exception as ex:
            Log.logger.critical(f'tt_info_sub_fn: exception: {ex}')

if __name__ == '__main__':
    '''Initialise logger'''
    if Log.logger is None:
        my_log = Log()

    '''Read Database Configuration'''
    scc_cfg = SccDlmConfRead()
    scc_cfg.read_cfg('../config/scc.conf')

    '''initialise DLM API and connect database'''
    scc_api = SccAPI()
    psql_db = scc_api.connect_database(scc_cfg)

    '''Create database model'''
    if psql_db:
        psql_db.create_tables([SectionInfo, DpInfo, SectionConfigInfo])
    else:
        pass

    '''Read simulator configuration'''
    section_config = SectionConfig()

    scc_api.init_section_connections_info()
    scc_api.init_train_trace_info()
    
    '''start MQTT client connection'''
    try:
        mqtt_client = MqttClient(
            scc_cfg.lmb['BROKER_IP_ADDRESS'],
            scc_cfg.lmb['PORT'],
            scc_cfg.scc_id,
            scc_cfg.lmb["USERNAME"],
            scc_cfg.lmb["PASSWORD"],
            scc_cfg.scc_id)

        mqtt_client.connect()
    except Exception as ex:
        Log.logger.critical(f'mqtt exception: {ex}')

    '''scc server'''
    scc_server = Sccserver(mqtt_client)
    scc_server.fill_yard_config_info_from_db()
    scc_server.fill_section_connections_info_from_db()

    '''point configuration'''
    scc_server.load_point_config()

    '''subscribe cwsm/section_reset mqtt topic'''
    mqtt_client.sub("cwsm/section_reset", scc_server.cwsm_section_reset_sub_fn)

    '''subscribe cwsm/reset_dp mqtt topic'''
    mqtt_client.sub("cwsm/dp_reset", scc_server.cwsm_dp_reset_sub_fn)
    
    '''subscribe sem/section_info mqtt topic'''
    mqtt_client.sub("sem/section_info",
                    scc_server.evaluator_section_info_sub_fn)

    '''subscribe mqtt topic'''
    mqtt_client.sub("scc/torpedo_info", scc_api.torpedo_info_sub_fn)

    '''subscribe sem/section_info mqtt topic'''
    mqtt_client.sub("pms/point_info",
                    scc_server.point_info_sub_fn)

    '''subscribe sem/section_info mqtt topic'''
    mqtt_client.sub("scc/trail_through",
                    scc_server.tt_info_sub_fn)

    '''subscribe sem/section_info mqtt topic'''
    mqtt_client.sub("cwsm/tt_clear",
                    scc_server.tt_clear_sub_fn)
    # while True:
    #     pass
    # removing the infinite loop and calling the mqtt_client loop
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    threading.Event().wait()

    #sys.exit(0)
