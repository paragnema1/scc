'''
*****************************************************************************
*File : scc_log.py
*Module : common 
*Purpose : Logger Library 
*Author : Sumankumar Panchal 
*Copyright : Copyright 2020, Lab to Market Innovations Private Limited
*****************************************************************************
'''

import logging
import time
import string
import logging.handlers as handlers

class Log:
    logger = None

    def __init__(self):
        Log.logger = logging.getLogger('SCC')
        Log.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            #'%(message)s')
            '%(levelname)s: %(asctime)s: %(filename)s: %(lineno)s:  %(message)s')

        # create console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        Log.logger.addHandler(ch)

        # create file handler
        logname = '../../log/scc/scc.log'
        fh = handlers.TimedRotatingFileHandler(logname, when='S', interval=60 * 60, backupCount=0)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        Log.logger.addHandler(fh)


if __name__ == '__main__':
    log = Log()
    i = 0
    Log.logger.critical(f'Test Critical message')
    while True:
        Log.logger.info(f'********************************** Test Print {i} ******************************************')
        time.sleep(1)
        i = i + 1
