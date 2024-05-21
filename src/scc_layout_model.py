'''
*****************************************************************************
*File : layout_model.py
*Module : SCC 
*Purpose : Database model class for design postgresql database to store data.
*Author : Sumankumar Panchal
*Copyright : Copyright 2020, Lab to Market Innovations Private Limited
*****************************************************************************
'''

'''Import python module'''

'''Import wild module'''
from scc_dlm_conf import *
from scc_log import *
from peewee import *
from datetime import datetime
import sys
sys.path.insert(1, "./common")

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

try:
    psql_db = PostgresqlDatabase(
        db_name, user=user, password=password, host=host, port=port)
    if psql_db != None:
        psql_db.connect()
except Exception as e:
    Log.logger.critical(f'occ_dlm_model: Exception: {e}')


class OccModel(Model):
    """A base model that will use our Postgresql database"""
    class Meta:
        database = psql_db


class LayoutSectionInfo(OccModel):
    ''' Layout section information table'''
    section_id = CharField()
    name = CharField()
    length = IntegerField()
    reverse_length = IntegerField()
    x1 = IntegerField()
    y1 = IntegerField()
    x2 = IntegerField()
    y2 = IntegerField()
    train_scale = IntegerField()
    force_train_scale = BooleanField()
    ignore_section_full = BooleanField()

    class Meta:
        table_name = "layout_section"


class LayoutSectionConnectionsInfo(OccModel):
    ''' Layout section connections information table '''
    section_id = CharField()
    left_normal = CharField()
    right_normal = CharField()
    left_reverse = CharField()
    right_reverse = CharField()

    class Meta:
        table_name = "layout_section_connections"


if __name__ == '__main__':
    if Log.logger is None:
        my_log = Log()
    Log.logger.info("layout_model: main program")

    psql_db.create_tables([LayoutSectionInfo, LayoutSectionConnectionsInfo])
