'''
*****************************************************************************
*File : scc_dlm_api.py
*Module : SCC 
*Purpose : SCC data logging module API class for database operations
*Author : Sumankumar Panchal
*Copyright : Copyright 2020, Lab to Market Innovations Private Limited
*****************************************************************************
'''

'''Import python packages'''

'''Import SCC packages '''
from scc_dlm_conf import *
from scc_log import *
import sys
import json
from peewee import *
from datetime import datetime, timedelta
from scc_dlm_model import *
from scc_layout_model import *
sys.path.insert(1, "./common")


TOTAL_SECTION_TRACE_FOR_TRAIN = 5
SECTION_TRACE_FOR_TRAIN_LIST = ["S1", "S2", "S12", "S13", "S14"]

ENTRY_EXIT_SECTION_LIST = ["S1", "S2"]
UNLOADING_SECTION_LIST = ["S12", "S13", "S14"]
MIDDLE_SECTION_LIST = [
    "S3",
    "S4",
    "S5",
    "S6",
    "S7",
    "S8",
    "S9",
    "S10",
    "S11"]


class TrainEntryExitTrace():
    def __init__(self):
        self.ts = 0.0
        self.section_id = "none"
        self.in_torpedo_axle_count = 0
        self.out_torpedo_axle_count = 0
        self.engine_axle_count = 0
        self.section_status = "none"
        self.direction = "none"
        self.speed = 0.0
        self.torpedo_status = "none"
        self.torpedo_id = 0
        self.engine_id = 0


class SectionConnections:
    def __init__(self):
        self.section_id = "none"
        self.left_normal = "none"
        self.right_normal = "none"
        self.left_reverse = "none"
        self.right_reverse = "none"
        self.torpedo_id = 0
        self.engine_id = 0
        self.in_torpedo_axle_count = 0
        self.out_torpedo_axle_count = 0
        self.entry_time = 0
        self.exit_time = 0
        self.torpedo_detected = False
        self.unloaded_entry_time = 0
        self.unloaded_exit_time = 0
        self.in_axles = 0
        self.out_axles = 0


class Torpedo:
    def __init__(self):
        self.section_id = "none"
        self.torpedo_id = 0
        self.engine_id = 0
        self.unloaded_entry_time = 0
        self.unloaded_exit_time = 0
        self.entry_time = 0
        self.exit_time = 0
        self.in_torpedo_axle_count = 0
        self.out_torpedo_axle_count = 0
        self.in_axles = 0
        self.out_axles = 0
        self.torpedo_detected = False


class SccAPI:
    '''OCC DAtabase operations such as Select, Insert, Delete records'''

    def __init__(self):
        self.train_trace_obj_list = []
        self.section_conn_obj_list = []
        self.torpedo_obj_list = []
        self.entry_torpedo_id = 0
        self.entry_engine_id = 0
        self.torpedo_id = 0
        self.engine_id = 0
        self.last_tt_record_inserted = {
            's3': False, 's4': False, 's7': False, 's8': False, 's11': False}

    def connect_database(self, config):
        '''Establish connection with database'''
        try:
            self.json_data = config.json_data
            self.db_name = self.json_data["DATABASE"]["DB_NAME"]
            self.user = self.json_data["DATABASE"]["USER"]
            self.password = self.json_data["DATABASE"]["PASSWORD"]
            self.host = self.json_data["DATABASE"]["HOST"]
            self.port = 5432

            if len(self.db_name) == 0:
                Log.logger.critical(
                    "scc_dlm_api: connect_database:  database name missing")
            else:
                psql_db = PostgresqlDatabase(
                    self.db_name,
                    user=self.user,
                    password=self.password,
                    host=self.host,
                    port=self.port)
                if psql_db:
                    try:
                        psql_db.connect()
                        Log.logger.info(
                            f'scc_dlm_api: database connection successful')
                        return psql_db
                    except Exception as e:
                        Log.logger.critical(
                            f'scc_dlm_api: connect_database: {e}')
                        sys.exit(1)
                else:
                    return None
        except Exception as ex:
            Log.logger.critical(
                "scc_dlm_api: connect_database: Exception: ", ex)

    def get_user_roles(self, username_param):
        '''get user roles from database'''
        try:
            user_details_table = OccUserInfo.select().where(
                OccUserInfo.username == username_param).get()
            Log.logger.info(
                f'Section reset request received from user:{username_param}, and user role is {user_details_table.roles[0]}')
            return user_details_table.roles
        except DoesNotExist:
            Log.logger.warning(
                f'Requested username does not exist in the database')
            return None

    def get_dpu_id(self, section_id_param):
        '''search dpu id of selected section_id'''
        try:
            yard_config_table = YardConfigInfo.select().where(
                YardConfigInfo.section_id == section_id_param).get()
            Log.logger.info(
                f'SECTION ID:{section_id_param} =>  DPU_ID: {yard_config_table.dpu_id}')
            return yard_config_table.dpu_id
        except DoesNotExist:
            Log.logger.warning(
                f'Requested DPU_ID does not exist in the database')
            return None

    def insert_section_info(self, data):
        ''' insert section information '''
        try:
            json_data = json.loads(data)

            '''list'''
            list_tuple = []
            d = []
            for i in range(len(json_data["sections"])):
                d.append(json_data["ts"])
                d.append(json_data["sections"][i]["section_id"])
                d.append(json_data["sections"][i]["section_status"])
                d.append(json_data["sections"][i]["engine_axle_count"])
                d.append(json_data["sections"][i]["torpedo_axle_count"])
                d.append(json_data["sections"][i]["direction"])
                d.append(json_data["sections"][i]["speed"])
                d.append(json_data["sections"][i]["torpedo_status"])
                d.append(json_data["sections"][i]["first_axle"])
                t = tuple(d)
                list_tuple.append(t)
                d.clear()

            SectionInfo.insert_many(
                list_tuple,
                fields=[
                    SectionInfo.ts,
                    SectionInfo.section_id,
                    SectionInfo.section_status,
                    SectionInfo.engine_axle_count,
                    SectionInfo.torpedo_axle_count,
                    SectionInfo.direction,
                    SectionInfo.speed,
                    SectionInfo.torpedo_status,
                    SectionInfo.first_axle]).execute()

        except Exception as ex:
            Log.logger.critical(
                f'scc_dlm_api: insert_section_info: exception:  {ex}')

    def insert_section_playback_info(self, data):
        ''' insert section information '''
        try:
            json_data = json.loads(data)

            section_playback_table = SectionPlaybackInfo()
            section_playback_table.ts = json_data["ts"]
            section_playback_table.sections = json_data['sections']

            section_playback_table.save()

        except Exception as ex:
            Log.logger.critical(
                f'scc_dlm_api: insert_section_info: exception:  {ex}')

    def read_section_playback_info(self):
        try:
            section_playback_model = SectionPlaybackInfo()
            section_playback_records = section_playback_model.select()

            for i in section_playback_records:
                section_msg = i.sections
                for j in range(len(section_msg)):
                    Log.logger.info(
                        f'ts:{i.ts},{section_msg[j]["section_id"]},{section_msg[j]["section_status"]}')
        except Exception as ex:
            Log.logger.critical(
                f'scc_dlm_api: read_section_playback_info: exception: {ex}')

    def insert_dp_info(self, data):
        ''' insert DP information '''
        try:
            json_data = json.loads(data)

            '''list'''
            list_tuple = []
            d = []
            for i in range(len(json_data["dps"])):
                d.append(json_data["ts"])
                d.append(json_data["dpu_id"])
                d.append(json_data["dps"][i]["dp_id"])
                d.append(json_data["dps"][i]["axle_count"])
                d.append(json_data["dps"][i]["axle_type"])
                d.append(json_data["dps"][i]["direction"])
                d.append(json_data["dps"][i]["speed"])
                t = tuple(d)
                list_tuple.append(t)
                d.clear()

            DpInfo.insert_many(
                list_tuple,
                fields=[
                    DpInfo.ts,
                    DpInfo.dpu_id,
                    DpInfo.dp_id,
                    DpInfo.axle_count,
                    DpInfo.axle_type,
                    DpInfo.direction,
                    DpInfo.speed]).execute()

        except Exception as ex:
            Log.logger.critical(
                f'scc_dlm_api: insert_section_info: exception:  {ex}')

    def select_section_info(self):
        '''Get store records from scc_section_info'''
        try:
            self.model_name = SectionInfo()
            self.records = self.model_name.select()
            return self.records
        except Exception as ex:
            Log.logger.critical(
                f'scc_dlm_api: select_section_info: exception: {ex}')

    def read_section_config_info(self):
        '''read section configuration information from database'''
        try:
            config_model_name = SectionConfigInfo()
            config_records = config_model_name.select()
            return config_records
        except Exception as ex:
            Log.logger.critical(
                f'scc_dlm_api: select_scc_config_info: exception: {ex}')

    def read_yard_config_info(self):
        '''read section configuration information from database'''
        try:
            yard_config_model = YardConfigInfo()
            yard_config_records = yard_config_model.select()
            return yard_config_records
        except Exception as ex:
            Log.logger.critical(
                f'scc_dlm_api: read_yard_config_info: exception: {ex}')

    def read_section_connections_info(self):
        '''read section configuration information from database'''
        try:
            section_connections_model = LayoutSectionConnectionsInfo()
            section_connections_records = section_connections_model.select()
            Log.logger.info(section_connections_records)
            return section_connections_records
        except Exception as ex:
            Log.logger.critical(
                f'scc_dlm_api: select_scc_config_info: exception: {ex}')

    def reset_train_trace_info(self):
        '''Reset train trace objects'''
        try:
            for i in range(len(self.train_trace_obj_list)):
                self.train_trace_obj_list[i].ts = 0.0
                self.train_trace_obj_list[i].in_torpedo_axle_count = 0
                self.train_trace_obj_list[i].out_torpedo_axle_count = 0
                self.train_trace_obj_list[i].engine_axle_count = 0
                self.train_trace_obj_list[i].section_status = "cleared"
                self.train_trace_obj_list[i].direction = "none"
                self.train_trace_obj_list[i].speed = 0.0
                self.train_trace_obj_list[i].torpedo_status = "none"
                self.train_trace_obj_list[i].torpedo_id = 0
                self.train_trace_obj_list[i].engine_id = 0

            Log.logger.info(f'reset train trace info...')
        except Exception as ex:
            Log.logger.critical(
                f'scc_dlm_api: init_train_movement_info: excpetion: {ex}')

    def insert_train_trace_info(self, data):
        '''insert section inform to trace train entry and exit'''
        try:
            json_data = json.loads(data)

            for i in range(TOTAL_SECTION_TRACE_FOR_TRAIN):
                for j in range(len(json_data['sections'])):
                    if json_data['sections'][j]['section_id'] == self.train_trace_obj_list[i].section_id:
                        '''compare section torpedo_axle_count is 16 or not'''
                        if json_data['sections'][j]['torpedo_axle_count'] == 16 and json_data['sections'][j]['direction'] == "in":
                            '''check previous section torpedo_axle_count is less than 16 or not'''
                            if self.train_trace_obj_list[i].in_torpedo_axle_count < 16:
                                '''insert record if previous section torpedo_axle_count less than 16 and current
                                torpedo_axle_count is 16'''
                                train_trace_table = TrainTraceInfo()
                                train_trace_table.ts = json_data["ts"]
                                train_trace_table.section_id = json_data["sections"][j]["section_id"]
                                train_trace_table.section_status = json_data["sections"][j]["section_status"]
                                train_trace_table.torpedo_axle_count = json_data[
                                    "sections"][j]["torpedo_axle_count"]
                                train_trace_table.engine_axle_count = json_data[
                                    "sections"][j]["engine_axle_count"]
                                train_trace_table.direction = json_data["sections"][j]["direction"]
                                train_trace_table.speed = json_data["sections"][j]["speed"]
                                train_trace_table.torpedo_status = json_data["sections"][j]["torpedo_status"]
                                train_trace_table.first_axle = json_data["sections"][j]["first_axle"]
                                self.entry_torpedo_id = self.entry_torpedo_id + 1
                                self.entry_engine_id = self.entry_engine_id + 1
                                train_trace_table.torpedo_id = self.entry_torpedo_id
                                train_trace_table.engine_id = self.entry_engine_id
                                train_trace_table.save()
                                '''save it in train_trace_obj_list'''
                                self.train_trace_obj_list[i].in_torpedo_axle_count = json_data['sections'][j]['torpedo_axle_count']
                            else:
                                self.train_trace_obj_list[i].in_torpedo_axle_count = json_data['sections'][j]['torpedo_axle_count']
                        else:
                            self.train_trace_obj_list[i].in_torpedo_axle_count = json_data['sections'][j]['torpedo_axle_count']

                        if json_data['sections'][j]['direction'] == "out" or json_data['sections'][j]['direction'] == "none":
                            if json_data['sections'][j]['torpedo_axle_count'] >= 1:
                                self.train_trace_obj_list[i].out_torpedo_axle_count = json_data['sections'][j]['torpedo_axle_count']
                            elif json_data['sections'][j]['torpedo_axle_count'] == 0:
                                if self.train_trace_obj_list[i].out_torpedo_axle_count > 0:
                                    train_trace_table = TrainTraceInfo()
                                    train_trace_table.ts = json_data["ts"]
                                    train_trace_table.section_id = json_data["sections"][j]["section_id"]
                                    train_trace_table.section_status = json_data[
                                        "sections"][j]["section_status"]
                                    train_trace_table.torpedo_axle_count = json_data[
                                        "sections"][j]["torpedo_axle_count"]
                                    train_trace_table.engine_axle_count = json_data[
                                        "sections"][j]["engine_axle_count"]
                                    train_trace_table.direction = json_data["sections"][j]["direction"]
                                    train_trace_table.speed = json_data["sections"][j]["speed"]
                                    train_trace_table.torpedo_status = json_data[
                                        "sections"][j]["torpedo_status"]
                                    train_trace_table.first_axle = json_data["sections"][j]["first_axle"]
                                    train_trace_table.torpedo_id = self.entry_torpedo_id
                                    train_trace_table.engine_id = self.entry_engine_id
                                    train_trace_table.save()
                                    self.train_trace_obj_list[i].out_torpedo_axle_count = 0
                                else:
                                    pass
                            else:
                                pass
                        else:
                            pass
                    else:
                        pass
        except Exception as ex:
            Log.logger.critical(
                f'scc_dlm_api: insert_train_trace_info: exception: {ex}')

    def init_section_connections_info(self):
        try:
            section_connections_db_records = self.read_section_connections_info()
            for sc in section_connections_db_records:
                Log.logger.info(
                    f'SECTION_ID: {sc.section_id}, LEFT_SECTION: {sc.left_normal}, RIGHT_SECTION: {sc.right_normal}')

            for sc in section_connections_db_records:
                self.section_conn_obj_list.append(SectionConnections())
                self.torpedo_obj_list.append(Torpedo())

            sc_idx = 0
            for sc in section_connections_db_records:
                self.section_conn_obj_list[sc_idx].section_id = sc.section_id
                self.section_conn_obj_list[sc_idx].left_normal = sc.left_normal
                self.section_conn_obj_list[sc_idx].right_normal = sc.right_normal
                self.section_conn_obj_list[sc_idx].left_reverse = sc.left_reverse
                self.section_conn_obj_list[sc_idx].right_reverse = sc.right_reverse
        
                self.torpedo_obj_list[sc_idx].section_id = sc.section_id
                sc_idx += 1
                
        except Exception as ex:
            Log.logger.critical(
                f'init_section_connections_info: exception {ex}')

    def init_train_trace_info(self):
        '''initialise train trace objects'''
        try:
            for i in range(TOTAL_SECTION_TRACE_FOR_TRAIN):
                self.train_trace_obj_list.append(TrainEntryExitTrace())

            for i in range(TOTAL_SECTION_TRACE_FOR_TRAIN):
                self.train_trace_obj_list[i].section_id = SECTION_TRACE_FOR_TRAIN_LIST[i]

            Log.logger.info(f'init train trace info...')
        except Exception as ex:
            Log.logger.critical(
                f'scc_dlm_api: init_train_movement_info: excpetion: {ex}')

    def update_torpedo_id(self, data):
        try:
            for sc_idx in range(len(self.section_conn_obj_list)):
                Log.logger.info(
                    f'{self.section_conn_obj_list[sc_idx].section_id}')
                if self.section_conn_obj_list[sc_idx].section_id == str(data["section_id"]):
                    self.section_conn_obj_list[sc_idx].torpedo_id = str(
                        data["torpedo_id"])
                    Log.logger.info(
                        f'Section : {section_id}, Torpedo ID : {torpedo_id} updated')
                else:
                    Log.logger.info(
                        f'update torpedo id : section id did not match')
                    pass
        except Exception as ex:
            Log.logger.critical(f'update_torpedo_id: exception : {ex}')

    def torpedo_info_sub_fn(self, in_client, user_data, message):
        try:
            Log.logger.info(f'torepdo_info mqtt msg received')
            msg_payload = json.loads(message.payload)

            self.update_torpedo_id(msg_payload)
        except Exception as ex:
            Log.logger.critical(f'torpedo_info_sub_fn: exception: {ex}')

    def reset_section_connections_info(self):
        try:
            for sc_idx in range(len(self.section_conn_obj_list)):
                self.section_conn_obj_list[sc_idx].torpedo_id = 0
                self.section_conn_obj_list[sc_idx].engine_id = 0
                self.section_conn_obj_list[sc_idx].in_torpedo_axle_count = 0
                self.section_conn_obj_list[sc_idx].out_torpedo_axle_count = 0
                self.section_conn_obj_list[sc_idx].entry_time = 0
                self.section_conn_obj_list[sc_idx].exit_time = 0
                self.section_conn_obj_list[sc_idx].torpedo_detected = False
        except Exception as ex:
            Log.logger.critical(
                f'init_section_connections_info: exception {ex}')

    def torpedo_performance(self, data):
        try:
            json_data = json.loads(data)
            section_list = {}

            for json_idx in range(len(json_data['sections'])):
                section_list[json_data['sections'][json_idx]
                             ['section_id']] = json_data['sections'][json_idx]

            for json_idx in range(len(json_data['sections'])):
                if json_data['sections'][json_idx]['section_id'] in UNLOADING_SECTION_LIST:
                    for sc_idx in range(len(self.torpedo_obj_list)):
                        if json_data['sections'][json_idx]['section_id'] == self.torpedo_obj_list[sc_idx].section_id:
                            if json_data['sections'][json_idx]['section_status'] != "none" or json_data[
                                    'sections'][json_idx]['direction'] != "none":
                                if json_data['sections'][json_idx]['torpedo_axle_count'] >= 12 and self.torpedo_obj_list[sc_idx].in_torpedo_axle_count < 12:
                                    self.torpedo_obj_list[sc_idx].unloaded_entry_time = json_data["ts"]
                                    self.torpedo_obj_list[sc_idx].torpedo_id = "T" + time.strftime('%d%m%Y%H%M%S', time.localtime(json_data["ts"]))
                                    self.torpedo_obj_list[sc_idx].engine_id = "E" + time.strftime('%d%m%Y%H%M%S', time.localtime(json_data["ts"]))
                                    
                                    Log.logger.info(
                                        f'Section_id : {self.section_conn_obj_list[sc_idx].section_id},'
                                        f'torpedo_id : {self.section_conn_obj_list[sc_idx].torpedo_id},'
                                        f'engine_id: {self.section_conn_obj_list[sc_idx].engine_id},'
                                        f'unloaded entry ts: {self.section_conn_obj_list[sc_idx].unloaded_entry_time}')

                                    self.torpedo_obj_list[sc_idx].in_torpedo_axle_count = json_data[
                                        'sections'][json_idx]['torpedo_axle_count']

                                    if self.torpedo_obj_list[sc_idx].torpedo_id != 0 and self.torpedo_obj_list[sc_idx].engine_id != 0:
                                        '''insert torpedo entry time while entrying unloading section'''
                                        self.insert_torpedo_loaded_entry_info(
                                            self.torpedo_obj_list[sc_idx].torpedo_id,
                                            self.torpedo_obj_list[sc_idx].engine_id,
                                            self.torpedo_obj_list[sc_idx].unloaded_entry_time,
                                            self.torpedo_obj_list[sc_idx].section_id)
                                    else:
                                        pass
                                else:
                                    self.torpedo_obj_list[sc_idx].in_torpedo_axle_count = json_data[
                                        'sections'][json_idx]['torpedo_axle_count']
                            else:
                                pass

                            '''-----------------------------------GET UNLOADING EXIT TIME-----------------------------------'''
                            if json_data['sections'][json_idx]['direction'] == "out" or json_data['sections'][json_idx]['direction'] == "none":
                                if json_data['sections'][json_idx]['torpedo_axle_count'] >= 6:
                                    self.torpedo_obj_list[sc_idx].out_torpedo_axle_count = json_data[
                                        'sections'][json_idx]['torpedo_axle_count']
                                else:
                                    pass
                                if self.torpedo_obj_list[sc_idx].out_torpedo_axle_count >= 6 and json_data[
                                        'sections'][json_idx]['torpedo_axle_count'] < 6:
                                    self.torpedo_obj_list[sc_idx].unloaded_exit_time = json_data["ts"]

                                    Log.logger.info(f'Section_id : {self.torpedo_obj_list[sc_idx].section_id},'
                                                    f'torpedo_id : {self.torpedo_obj_list[sc_idx].torpedo_id},'
                                                    f'engine_id: {self.torpedo_obj_list[sc_idx].engine_id},'
                                                    f'unloaded exit ts: {self.torpedo_obj_list[sc_idx].unloaded_exit_time}')

                                    '''do not update db when torpedo id and engine id is 0'''
                                    if self.torpedo_obj_list[sc_idx].torpedo_id != 0 and self.torpedo_obj_list[sc_idx].engine_id != 0:
                                        self.update_torpedo_unloaded_exit_info(
                                            self.torpedo_obj_list[sc_idx].torpedo_id,
                                            self.torpedo_obj_list[sc_idx].engine_id,
                                            self.torpedo_obj_list[sc_idx].unloaded_exit_time,
                                            self.torpedo_obj_list[sc_idx].section_id)
                                    else:
                                        pass

                                    self.torpedo_obj_list[sc_idx].out_torpedo_axle_count = 0
                                else:
                                    pass
                            else:
                                pass

                            Log.logger.info(
                                f'Section_id : {self.torpedo_obj_list[sc_idx].section_id},'
                                f'torpedo_id : {self.torpedo_obj_list[sc_idx].torpedo_id},'
                                f'engine_id: {self.torpedo_obj_list[sc_idx].engine_id},')
                                #f'in: {self.torpedo_obj_list[sc_idx].in_torpedo_axle_count},'
                                #f'out: {self.torpedo_obj_list[sc_idx].out_torpedo_axle_count}')
                        else:
                            pass
                else:
                    pass
        except Exception as ex:
            Log.logger.critical(f'torpedo_performance: exception {ex}')

    def yard_performance(self, data):
        try:
            json_data = json.loads(data)
            section_list = {}

            for json_idx in range(len(json_data['sections'])):
                section_list[json_data['sections'][json_idx]
                             ['section_id']] = json_data['sections'][json_idx]

            for json_idx in range(len(json_data['sections'])):

                '''--------------------------------------ENTRY EXIT SECTION LOGIC ------------------------------'''
                if json_data['sections'][json_idx]['section_id'] in ENTRY_EXIT_SECTION_LIST:
                    for sc_idx in range(len(self.section_conn_obj_list)):
                        if json_data['sections'][json_idx]['section_id'] == self.section_conn_obj_list[sc_idx].section_id:

                            '''--------------------------GET TRAIN ENTRY TIME------------------------------'''
                            if json_data['sections'][json_idx]['section_status'] == "occupied" and json_data[
                                    'sections'][json_idx]['direction'] == "in":
                                if json_data['sections'][json_idx]['torpedo_axle_count'] >= 12 and self.section_conn_obj_list[sc_idx].in_torpedo_axle_count < 12:

                                    self.torpedo_id = "T" + \
                                        time.strftime(
                                            '%d%m%Y%H%M%S', time.localtime(json_data["ts"]))
                                    self.engine_id = "E" + \
                                        time.strftime(
                                            '%d%m%Y%H%M%S', time.localtime(json_data["ts"]))

                                    self.section_conn_obj_list[sc_idx].torpedo_id = self.torpedo_id
                                    self.section_conn_obj_list[sc_idx].engine_id = self.engine_id
                                    self.section_conn_obj_list[sc_idx].entry_time = json_data["ts"]

                                    if self.section_conn_obj_list[sc_idx].torpedo_id != 0 and self.section_conn_obj_list[sc_idx].engine_id != 0:
                                        '''insert new train entry in db'''
                                        self.insert_train_entry_info(
                                            self.torpedo_id, self.engine_id, json_data["ts"])
                                    else:
                                        pass

                                    if self.section_conn_obj_list[sc_idx].left_normal != "NONE":
                                        sec_id = self.section_conn_obj_list[sc_idx].left_normal

                                    if self.section_conn_obj_list[sc_idx].right_normal != "NONE":
                                        sec_id = self.section_conn_obj_list[sc_idx].right_normal

                                    Log.logger.info(
                                        f'Section_id : {self.section_conn_obj_list[sc_idx].section_id},'
                                        f'torpedo_id : {self.section_conn_obj_list[sc_idx].torpedo_id},'
                                        f'engine_id: {self.section_conn_obj_list[sc_idx].engine_id},'
                                        f'entry ts: {self.section_conn_obj_list[sc_idx].entry_time}')

                                    self.section_conn_obj_list[sc_idx].in_torpedo_axle_count = json_data[
                                        'sections'][json_idx]['torpedo_axle_count']
                                else:
                                    self.section_conn_obj_list[sc_idx].in_torpedo_axle_count = json_data[
                                        'sections'][json_idx]['torpedo_axle_count']
                            else:
                                pass

                            '''-------------------------------------GET TRAIN EXIT TIME---------------------------------'''

                            if json_data['sections'][json_idx]['direction'] == "out":

                                sec_id = self.section_conn_obj_list[sc_idx].left_normal

                                for section_idx in range(len(self.section_conn_obj_list)):
                                    if self.section_conn_obj_list[section_idx].section_id == sec_id:
                                        if self.section_conn_obj_list[section_idx].torpedo_id != 0:
                                            self.section_conn_obj_list[
                                                sc_idx].torpedo_id = self.section_conn_obj_list[section_idx].torpedo_id
                                            self.section_conn_obj_list[
                                                sc_idx].engine_id = self.section_conn_obj_list[section_idx].engine_id
                                        else:
                                            pass
                                    else:
                                        pass

                                if json_data['sections'][json_idx]['torpedo_axle_count'] >= 6:
                                    self.section_conn_obj_list[sc_idx].out_torpedo_axle_count = json_data[
                                        'sections'][json_idx]['torpedo_axle_count']
                                else:
                                    pass

                                '''update train exit time in db'''
                                if self.section_conn_obj_list[sc_idx].out_torpedo_axle_count >= 6 and json_data['sections'][json_idx]['torpedo_axle_count'] <= 6:

                                    Log.logger.info(
                                        'torpedo exiting detected!!')
                                    self.section_conn_obj_list[sc_idx].exit_time = json_data["ts"]

                                    if self.section_conn_obj_list[sc_idx].torpedo_id != 0 and self.section_conn_obj_list[sc_idx].engine_id != 0:
                                        Log.logger.info(
                                            f'updated train exit info')
                                        self.update_train_exit_info(
                                            self.section_conn_obj_list[sc_idx].torpedo_id,
                                            self.section_conn_obj_list[sc_idx].engine_id,
                                            self.section_conn_obj_list[sc_idx].exit_time)

                                        self.section_conn_obj_list[sc_idx].out_torpedo_axle_count = 0
                                        self.section_conn_obj_list[sc_idx].in_torpedo_axle_count = 0
                                    else:
                                        pass

                                    Log.logger.info(
                                        f'Section_id : {self.section_conn_obj_list[sc_idx].section_id},'
                                        f'torpedo_id : {self.section_conn_obj_list[sc_idx].torpedo_id},'
                                        f'engine_id: {self.section_conn_obj_list[sc_idx].engine_id},'
                                        f'train exit ts: {self.section_conn_obj_list[sc_idx].exit_time}')

                                else:
                                    pass
                            else:
                                pass

                            Log.logger.info(
                                f'Section_id : {self.section_conn_obj_list[sc_idx].section_id},'
                                f'torpedo_id : {self.section_conn_obj_list[sc_idx].torpedo_id},'
                                f'engine_id: {self.section_conn_obj_list[sc_idx].engine_id},')
                                #f'in: {self.section_conn_obj_list[sc_idx].in_torpedo_axle_count},'
                                #f'out: {self.section_conn_obj_list[sc_idx].out_torpedo_axle_count}')
                        else:
                            pass
                else:
                    pass

                '''------------------------------------------MIDDLE SECTIONS LOGIC---------------------------------------'''
                if json_data['sections'][json_idx]['section_id'] in MIDDLE_SECTION_LIST:
                    for sc_idx in range(len(self.section_conn_obj_list)):
                        if json_data['sections'][json_idx]['section_id'] == self.section_conn_obj_list[sc_idx].section_id:
                            if json_data['sections'][json_idx]['section_status'] == "occupied" and json_data[
                                    'sections'][json_idx]['direction'] != "none":

                                if json_data['sections'][json_idx]['direction'] == 'in':
                                    self.section_conn_obj_list[sc_idx].in_torpedo_axle_count = json_data[
                                        'sections'][json_idx]['torpedo_axle_count']
                                elif json_data['sections'][json_idx]['direction'] == 'out':
                                    self.section_conn_obj_list[sc_idx].out_torpedo_axle_count = json_data[
                                        'sections'][json_idx]['torpedo_axle_count']
                                else:
                                    pass

                                if json_data['sections'][json_idx]['direction'] == "out":
                                    if self.section_conn_obj_list[sc_idx].left_normal != "NONE":
                                        sec_id = self.section_conn_obj_list[sc_idx].left_normal

                                        for section_idx in range(
                                                len(self.section_conn_obj_list)):
                                            if self.section_conn_obj_list[section_idx].section_id == sec_id and section_list[
                                                    sec_id]["section_status"] == "occupied":
                                                if section_list[sec_id]["direction"] == "out":
                                                    self.section_conn_obj_list[
                                                        sc_idx].torpedo_id = self.section_conn_obj_list[section_idx].torpedo_id
                                                    self.section_conn_obj_list[
                                                        sc_idx].engine_id = self.section_conn_obj_list[section_idx].engine_id

                                                else:
                                                    pass
                                            else:
                                                pass
                                    else:
                                        pass

                                    if self.section_conn_obj_list[sc_idx].left_reverse != "NONE":
                                        sec_id = self.section_conn_obj_list[sc_idx].left_reverse

                                        for section_idx in range(
                                                len(self.section_conn_obj_list)):
                                            if self.section_conn_obj_list[section_idx].section_id == sec_id and section_list[
                                                    sec_id]["section_status"] == "occupied":
                                                if section_list[sec_id]["direction"] == "out":
                                                    self.section_conn_obj_list[
                                                        sc_idx].torpedo_id = self.section_conn_obj_list[section_idx].torpedo_id
                                                    self.section_conn_obj_list[
                                                        sc_idx].engine_id = self.section_conn_obj_list[section_idx].engine_id
                                                else:
                                                    pass
                                            else:
                                                pass
                                    else:
                                        pass
                                else:
                                    pass

                                if json_data['sections'][json_idx]['direction'] == "in":
                                    if self.section_conn_obj_list[sc_idx].right_normal != "NONE":
                                        sec_id = self.section_conn_obj_list[sc_idx].right_normal

                                        for section_idx in range(
                                                len(self.section_conn_obj_list)):
                                            if self.section_conn_obj_list[section_idx].section_id == sec_id and section_list[
                                                    sec_id]["section_status"] == "occupied":
                                                if section_list[sec_id]["direction"] == "in":
                                                    self.section_conn_obj_list[
                                                        sc_idx].torpedo_id = self.section_conn_obj_list[section_idx].torpedo_id
                                                    self.section_conn_obj_list[
                                                        sc_idx].engine_id = self.section_conn_obj_list[section_idx].engine_id
                                                else:
                                                    pass
                                            else:
                                                pass
                                    else:
                                        pass

                                    if self.section_conn_obj_list[sc_idx].right_reverse != "NONE":
                                        sec_id = self.section_conn_obj_list[sc_idx].right_reverse

                                        for section_idx in range(
                                                len(self.section_conn_obj_list)):
                                            if self.section_conn_obj_list[section_idx].section_id == sec_id and section_list[
                                                    sec_id]["section_status"] == "occupied":
                                                if section_list[sec_id]["direction"] == "in":
                                                    self.section_conn_obj_list[
                                                        sc_idx].torpedo_id = self.section_conn_obj_list[section_idx].torpedo_id
                                                    self.section_conn_obj_list[
                                                        sc_idx].engine_id = self.section_conn_obj_list[section_idx].engine_id
                                                else:
                                                    pass
                                            else:
                                                pass
                                    else:
                                        pass
                                else:
                                    pass

                            else:
                                pass

                            Log.logger.info(
                                f'Section_id : {self.section_conn_obj_list[sc_idx].section_id},'
                                f'torpedo_id : {self.section_conn_obj_list[sc_idx].torpedo_id},'
                                f'engine_id: {self.section_conn_obj_list[sc_idx].engine_id},')
                                #f'in: {self.section_conn_obj_list[sc_idx].in_torpedo_axle_count},'
                                #f'out: {self.section_conn_obj_list[sc_idx].out_torpedo_axle_count}')
                        else:
                            pass
                else:
                    pass

                if json_data['sections'][json_idx]['section_id'] in UNLOADING_SECTION_LIST:
                    for sc_idx in range(len(self.section_conn_obj_list)):
                        if json_data['sections'][json_idx]['section_id'] == self.section_conn_obj_list[sc_idx].section_id:
                            if json_data['sections'][json_idx]['section_status'] != "none" or json_data[
                                    'sections'][json_idx]['direction'] != "none":
                                if json_data['sections'][json_idx]['torpedo_axle_count'] >= 12 and self.section_conn_obj_list[sc_idx].in_torpedo_axle_count < 12:
                                    self.section_conn_obj_list[sc_idx].unloaded_entry_time = json_data["ts"]

                                    if self.section_conn_obj_list[sc_idx].left_normal != "NONE":
                                        sec_id = self.section_conn_obj_list[sc_idx].left_normal

                                        for section_idx in range(
                                                len(self.section_conn_obj_list)):
                                            if self.section_conn_obj_list[section_idx].section_id == sec_id and section_list[
                                                    sec_id]["section_status"] != "none":
                                                self.section_conn_obj_list[
                                                    sc_idx].torpedo_id = self.section_conn_obj_list[section_idx].torpedo_id
                                                self.section_conn_obj_list[
                                                    sc_idx].engine_id = self.section_conn_obj_list[section_idx].engine_id
                                            else:
                                                pass

                                    if self.section_conn_obj_list[sc_idx].right_normal != "NONE":
                                        sec_id = self.section_conn_obj_list[sc_idx].right_normal

                                        for section_idx in range(
                                                len(self.section_conn_obj_list)):
                                            if self.section_conn_obj_list[section_idx].section_id == sec_id and section_list[
                                                    sec_id]["section_status"] != "none":
                                                self.section_conn_obj_list[
                                                    sc_idx].torpedo_id = self.section_conn_obj_list[section_idx].torpedo_id
                                                self.section_conn_obj_list[
                                                    sc_idx].engine_id = self.section_conn_obj_list[section_idx].engine_id
                                            else:
                                                pass
                                    Log.logger.info(
                                        f'Section_id : {self.section_conn_obj_list[sc_idx].section_id},'
                                        f'torpedo_id : {self.section_conn_obj_list[sc_idx].torpedo_id},'
                                        f'engine_id: {self.section_conn_obj_list[sc_idx].engine_id},'
                                        f'unloaded entry ts: {self.section_conn_obj_list[sc_idx].unloaded_entry_time}')

                                    self.section_conn_obj_list[sc_idx].in_torpedo_axle_count = json_data[
                                        'sections'][json_idx]['torpedo_axle_count']

                                    if self.section_conn_obj_list[sc_idx].torpedo_id != 0 and self.section_conn_obj_list[sc_idx].engine_id != 0:
                                        '''update train entry time while entrying unloading section'''
                                        self.update_train_unloaded_entry_info(
                                            self.section_conn_obj_list[sc_idx].torpedo_id,
                                            self.section_conn_obj_list[sc_idx].engine_id,
                                            self.section_conn_obj_list[sc_idx].unloaded_entry_time,
                                            self.section_conn_obj_list[sc_idx].section_id)
                                    else:
                                        pass
                                else:
                                    self.section_conn_obj_list[sc_idx].in_torpedo_axle_count = json_data[
                                        'sections'][json_idx]['torpedo_axle_count']
                            else:
                                pass

                            '''-----------------------------------GET UNLOADING EXIT TIME-----------------------------------'''
                            if json_data['sections'][json_idx]['direction'] == "out" or json_data['sections'][json_idx]['direction'] == "none":
                                if json_data['sections'][json_idx]['torpedo_axle_count'] >= 6:
                                    self.section_conn_obj_list[sc_idx].out_torpedo_axle_count = json_data[
                                        'sections'][json_idx]['torpedo_axle_count']
                                else:
                                    pass
                                if self.section_conn_obj_list[sc_idx].out_torpedo_axle_count >= 6 and json_data[
                                        'sections'][json_idx]['torpedo_axle_count'] < 6:
                                    self.section_conn_obj_list[sc_idx].unloaded_exit_time = json_data["ts"]

                                    Log.logger.info(f'Section_id : {self.section_conn_obj_list[sc_idx].section_id},'
                                                    f'torpedo_id : {self.section_conn_obj_list[sc_idx].torpedo_id},'
                                                    f'engine_id: {self.section_conn_obj_list[sc_idx].engine_id},'
                                                    f'unloaded exit ts: {self.section_conn_obj_list[sc_idx].unloaded_exit_time}')

                                    '''do not update db when torpedo id and engine id is 0'''
                                    if self.section_conn_obj_list[sc_idx].torpedo_id != 0 and self.section_conn_obj_list[sc_idx].engine_id != 0:
                                        self.update_train_unloaded_exit_info(
                                            self.section_conn_obj_list[sc_idx].torpedo_id,
                                            self.section_conn_obj_list[sc_idx].engine_id,
                                            self.section_conn_obj_list[sc_idx].unloaded_exit_time,
                                            self.section_conn_obj_list[sc_idx].section_id)
                                    else:
                                        pass

                                    self.section_conn_obj_list[sc_idx].out_torpedo_axle_count = 0
                                else:
                                    pass
                            else:
                                pass

                            Log.logger.info(
                                f'Section_id : {self.section_conn_obj_list[sc_idx].section_id},'
                                f'torpedo_id : {self.section_conn_obj_list[sc_idx].torpedo_id},'
                                f'engine_id: {self.section_conn_obj_list[sc_idx].engine_id},')
                                #f'in: {self.section_conn_obj_list[sc_idx].in_torpedo_axle_count},'
                                #f'out: {self.section_conn_obj_list[sc_idx].out_torpedo_axle_count}')
                        else:
                            pass
                else:
                    pass

        except Exception as ex:
            Log.logger.critical(
                f'scc_dlm_api: insert_yard_performance: exception: {ex}')

    def insert_torpedo_loaded_entry_info(self, torpedo_id, engine_id, unloaded_entry_time, unloaded_section_id):
        try:
            torpedo_performance_table = TorpedoPerformanceInfo()
            torpedo_performance_table.engine_id = engine_id
            torpedo_performance_table.torpedo_id = torpedo_id
            torpedo_performance_table.unload_entry_ts = unloaded_entry_time
            torpedo_performance_table.unload_section_id = unloaded_section_id
            torpedo_performance_table.save()
            Log.logger.info(
                f'inserted torpedo loaded entry time: {entry_time}, torpedo_id: {torpedo_id}')
        except Exception as ex:
            Log.logger.critical(f'insert_torpedo_loaded_entry_info: exception: {ex}')

    def update_torpedo_unloaded_exit_info(self, torpedo_id, engine_id, unloaded_exit_time, unloaded_section_id):
        try:
            torpedo_performance_table = TorpedoPerformanceInfo.select().where(
                TorpedoPerformanceInfo.torpedo_id == torpedo_id).get()
            torpedo_performance_table.unload_exit_ts = unloaded_exit_time
            torpedo_performance_table.unload_section_id = unloaded_section_id
            torpedo_performance_table.save()
            Log.logger.info(
                f'inserted unloading zone torpedo exit time: {unloaded_exit_time}, torpedo_id: {torpedo_id}')
        except DoesNotExist:
            Log.logger.critical(
                f'update_torpedo_unloaded_exit_info: record does not exist')

    def insert_train_entry_info(self, torpedo_id, engine_id, entry_time):
        try:
            yard_performance_table = YardPerformanceInfo()
            yard_performance_table.engine_id = engine_id
            yard_performance_table.torpedo_id = torpedo_id
            yard_performance_table.entry_ts = entry_time
            yard_performance_table.save()
            Log.logger.info(
                f'inserted train entry time: {entry_time}, torpedo_id: {torpedo_id}')
        except Exception as ex:
            Log.logger.critical(f'insert_train_entry_info: exception: {ex}')

    def update_train_exit_info(self, torpedo_id, engine_id, exit_time):
        try:
            yard_performance_table = YardPerformanceInfo.select().where(
                YardPerformanceInfo.torpedo_id == torpedo_id).get()
            yard_performance_table.exit_ts = exit_time
            yard_performance_table.save()
            Log.logger.info(
                f'inserted train exit time: {exit_time}, torpedo_id: {torpedo_id}')
        except DoesNotExist:
            Log.logger.critical(
                f'update_train_exit_info: record does not exist')

    def update_train_unloaded_entry_info(
            self, torpedo_id, engine_id, unloaded_entry_time, unloaded_section_id):
        try:
            yard_performance_table = YardPerformanceInfo.select().where(
                YardPerformanceInfo.torpedo_id == torpedo_id).get()
            yard_performance_table.unload_entry_ts = unloaded_entry_time
            yard_performance_table.unload_section_id = unloaded_section_id
            yard_performance_table.save()
            Log.logger.info(
                f'inserted unloading zone train entry time: {unloaded_entry_time}, torpedo_id: {torpedo_id}')
        except DoesNotExist:
            Log.logger.critical(
                f'update_train_unloaded_entry_info: record does not exist')

    def update_train_unloaded_exit_info(
            self, torpedo_id, engine_id, unloaded_exit_time, unloaded_section_id):
        try:
            yard_performance_table = YardPerformanceInfo.select().where(
                YardPerformanceInfo.torpedo_id == torpedo_id).get()
            yard_performance_table.unload_exit_ts = unloaded_exit_time
            yard_performance_table.unload_section_id = unloaded_section_id
            yard_performance_table.save()
            Log.logger.info(
                f'inserted unloading zone train exit time: {unloaded_exit_time}, torpedo_id: {torpedo_id}')
        except DoesNotExist:
            Log.logger.critical(
                f'update_train_unloaded_exit_info: record does not exist')

    def insert_event_info(self, event_ts, event_id, event_desc):
        try:
            event_table = EventInfo()
            event_table.ts = event_ts
            event_table.event_id = event_id
            event_table.event_desc = event_desc
            event_table.save()
        except Exception as ex:
            Log.logger.critical(f'insert_event_info: exception: {ex}')

    def insert_trail_through_info(self, tt_msg):
        '''insert data into trail through table'''
        try:
            if self.last_tt_record_inserted[tt_msg['section_id']] == False:
                tt_table = TrailThroughInfo()
                tt_table.tt_ts = tt_msg['ts']
                tt_table.section_id = tt_msg['section_id']
                tt_table.confirm_status = False
                tt_table.save()
                self.last_tt_record_inserted[tt_msg['section_id']
                                             ] = True
                Log.logger.info(f'insert trail through: {self.last_tt_record_inserted[tt_msg["section_id"]]}')

                '''insert data into trail through playback table'''
                tt_playback_table = TrailThroughPlayback()
                tt_playback_table.ts = tt_msg['ts']
                tt_playback_table.section_id = {"ts": tt_msg['ts'], "section_id": tt_msg['section_id'], "confirm_status": False}
                tt_playback_table.save()
            else:
                pass
        except Exception as ex:
            Log.logger.critical(f'insert_trail_through_info: exception: {ex}')

    def clear_trail_through(self, tt_msg):
        '''clear trail through alert when user sent mqtt message'''
        try:
            if self.last_tt_record_inserted[tt_msg['section_id']] == True:
                self.last_tt_record_inserted[tt_msg['section_id']] = False
                Log.logger.info(f'clear trail through: {self.last_tt_record_inserted[tt_msg["section_id"]]}')

                '''insert data into trail through playback table'''
                tt_playback_table = TrailThroughPlayback()
                tt_playback_table.ts = time.time()
                tt_playback_table.section_id = {"ts": time.time(), "section_id": tt_msg['section_id'], "confirm_status": True}
                tt_playback_table.save()
            else:
                pass
        except Exception as ex:
            Log.logger.critical(f'clear_trail_through: exception: {ex}')

if __name__ == '__main__':
    if Log.logger is None:
        my_log = Log()

    cfg = SccDlmConfRead()
    cfg.read_cfg('../config/scc.conf')

    scc_api = SccAPI()
    db_conn = scc_api.connect_database(cfg)

    if db_conn:
        scc_api.read_section_config_info()
        scc_api.read_section_connections_info()
        # scc_api.read_section_playback_info()
        scc_api.init_train_trace_info()
        scc_api.init_section_connections_info()
    else:
        pass