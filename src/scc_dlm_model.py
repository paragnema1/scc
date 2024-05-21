'''
*****************************************************************************
*File : scc_model.py
*Module : scc_dlm
*Purpose : Database model class for design postgresql database to store data.
*Author : Sumankumar Panchal
*Copyright : Copyright 2020, Lab to Market Innovations Private Limited
*****************************************************************************
'''

'''Import python module'''

'''Import wild module'''
from scc_dlm_conf import *
from scc_log import *
from peewee import * #module to write sql query in python
from datetime import datetime
import sys
from playhouse.postgres_ext import * #module for some postgresql specific data types
sys.path.insert(1, "./common") #adding common makes them import as they are installed modules

if Log.logger is None:
    my_log = Log()

'''read configuration file'''
cfg = SccDlmConfRead()
cfg.read_cfg('../config/scc.conf')

json_data = cfg.json_data
db_name = json_data["DATABASE"]["DB_NAME"]
user = json_data["DATABASE"]["USER"]
password = json_data["DATABASE"]["PASSWORD"]
host = json_data["DATABASE"]["HOST"]
port = 5432
psql_db = None

try: #block to connect postgrsql to database
    psql_db = PostgresqlDatabase(
        db_name, user=user, password=password, host=host, port=port)
    if psql_db != None:
        psql_db.connect() #connect postgresql to database
except Exception as e: #if any exception occurs then it will show error
    Log.logger.critical(f'scc_dlm_model: Exception: {e}')


class SccModel(Model):
    """A base model that will use our Postgresql database"""
    class Meta:
        database = psql_db


class SectionConfigInfo(SccModel):
    ''' SCC configuration table '''
    section_id = CharField()
    section_type = CharField()
    dp_id = CharField()
    dp_direction = CharField()

    class Meta:
        table_name = "occ_config"


class DpInfo(SccModel):
    ''' DP information table '''
    ts = DoubleField()
    dpu_id = CharField()
    dp_id = CharField()
    axle_count = IntegerField(null=True)
    axle_type = CharField(null=True)
    direction = CharField(null=True)
    speed = FloatField(null=True)

    class Meta:
        table_name = "dp"


class SectionInfo(SccModel):
    ''' Section information table '''
    ts = DoubleField()
    section_id = CharField()
    section_status = CharField(null=True)
    engine_id = CharField(null=True)
    torpedo_id = CharField(null=True)
    engine_axle_count = IntegerField(null=True)
    torpedo_axle_count = IntegerField(null=True)
    direction = CharField(null=True)
    speed = FloatField(null=True)
    torpedo_status = CharField(null=True)
    first_axle = CharField(null=True)

    class Meta:
        table_name = "section"


class SectionPlaybackInfo(SccModel):
    ''' Section information table '''
    ts = DoubleField()
    sections = JSONField()

    class Meta:
        table_name = "section_playback"


class TrainTraceInfo(SccModel):
    ''' Section information table '''
    ts = DoubleField()
    section_id = CharField()
    section_status = CharField(null=True)
    engine_id = CharField(null=True)
    torpedo_id = CharField(null=True)
    engine_axle_count = IntegerField(null=True)
    torpedo_axle_count = IntegerField(null=True)
    direction = CharField(null=True)
    speed = FloatField(null=True)
    torpedo_status = CharField(null=True)
    first_axle = CharField(null=True)

    class Meta:
        table_name = "train_trace"


class YardPerformanceInfo(SccModel):
    ''' Yard performance information table'''
    engine_id = CharField(null=True)
    torpedo_id = CharField(null=True)
    entry_ts = DoubleField(null=True)
    exit_ts = DoubleField(null=True)
    unload_entry_ts = DoubleField(null=True)
    unload_exit_ts = DoubleField(null=True)
    unload_section_id = CharField(null=True)

    class Meta:
        table_name = "yard_performance"


class TorpedoPerformanceInfo(SccModel):
    '''Torpedo performance information table'''
    engine_id = CharField(null=True)
    torpedo_id = CharField(null=True)
    entry_ts = DoubleField(null=True)
    exit_ts = DoubleField(null=True)
    unload_entry_ts = DoubleField(null=True)
    unload_exit_ts = DoubleField(null=True)
    unload_section_id = CharField(null=True)

    class Meta:
        table_name = "torpedo_performance"


class YardConfigInfo(SccModel):
    '''yard configuration'''
    yard_id = IntegerField()
    yard_name = CharField()
    dpu_id = CharField()
    dpu_name = CharField()
    section_id = CharField()
    section_name = CharField()
    dp_id = ArrayField(CharField, null = True)
    class Meta:
        table_name = "yard_config"


class OccUserInfo(SccModel):
    '''User authentication'''
    username = CharField()
    password = CharField()
    email = CharField()
    firstname = CharField()
    lastname = CharField(null=True)
    roles = ArrayField(CharField)
    profile_image = CharField(null=True)

    class Meta:
        table_name = "user_details"


class EventInfo(SccModel):
    ''' Event information table '''
    ts = DoubleField()
    event_id = CharField(null=True)
    event_desc = TextField(null=True)

    class Meta:
        table_name = "event"


class PointConfig(SccModel):
    section_id = CharField()
    point_id = CharField()

    class Meta:
        table_name = "pms_config"


class TrailThroughInfo(SccModel):
    tt_ts = DoubleField(null=True)
    section_id = CharField(null=True)
    confirm_status = BooleanField(default='FALSE')

    class Meta:
        table_name = "trail_through"


class TrailThroughPlayback(SccModel):
    ts = DoubleField(null=True)
    section_id = JSONField(null=True)

    class Meta:
        table_name = "trail_through_playback"


if __name__ == '__main__': #check if code is run directly by python interpreter
    if Log.logger is None:
        my_log = Log()
    Log.logger.info("scc_model: main program")

    #psql_db.create_tables([TorpedoPerformanceInfo])
    psql_db.create_tables([YardConfigInfo]) 
    #psql_db.create_tables([SectionConfigInfo, DpInfo, SectionInfo, TrainTraceInfo,
    #                       SectionPlaybackInfo, YardPerformanceInfo, YardConfigInfo, EventInfo, TrailThroughInfo, TrailThroughPlayback])
