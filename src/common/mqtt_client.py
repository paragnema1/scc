'''
*****************************************************************************
*File : MqttClient.py
*Module : common 
*Purpose : MQTT client Library 
*Author : Dhanaseelan Thangavel 
*Copyright : Copyright 2020, Lab to Market Innovations Private Limited
*****************************************************************************
'''

import sys
import traceback

import paho.mqtt.client as mqtt
import threading
import time
from collections import deque

from scc_log import Log


class MqttClient:

    def __init__(self, ipaddr, port, clientid, username='', password='', name='MQTT'):
        self.name = name
        self.broker_ip = ipaddr
        self.broker_port = port
        self.client_id = clientid
        self.user_name = username
        self.pwd = password
        self.sub_cbak_fn = {}
        self.pub_msg_queue = deque()
        self.is_connected = False
        self.con_error = False
        self.retry = False
        self.thread_started = False
        self.thread_quit = False
        self.th = None
        self.manual_discon = False
        if Log.logger is None:
            Log()
        self.client = mqtt.Client(clientid, clean_session=True, userdata=None)
        Log.logger.info(f'{self.name}: Connecting to Broker IP: {ipaddr}  portNo: {port} client_id: {clientid}')

    def __del__(self):
        Log.logger.info(f'{self.name}: Destructor is called client_id: {self.client_id}')

    def connect(self):
        try:
            self.setup_pre_con_params()
            self.client.connect(self.broker_ip, self.broker_port, 60)
            self.client.loop_start()
            while not self.is_connected and not self.con_error:
                pass
            if self.is_connected:
                Log.logger.warning(f'{self.name}: ***** CONNECT MQTT broker : {self.broker_ip}  Success *****')
                self.setup_post_con_params()
                self.thread_started = False
            if self.con_error:
                Log.logger.error(f'{self.name}: ***** Unable to CONNECT to  MQTT broker : {self.broker_ip}'
                                 f'Retrying ******')
                if not self.thread_started:
                    self.start_reconnect_th()
        except:
            Log.logger.error(
                f'{self.name}: ***** Unable to CONNECT to  MQTT broker : {self.broker_ip} Retrying.  Error: '
                f'{sys.exc_info()[0]} *****')
            if not self.thread_started:
                self.start_reconnect_th()

    def setup_pre_con_params(self):
        self.client.loop_stop()
        self.is_connected = False
        self.con_error = False
        self.manual_discon = False
        self.client.on_connect = self.on_con
        self.client.on_disconnect = self.on_discon
        self.client.on_message = self.on_msg
        if self.user_name != '':
            self.client.username_pw_set(self.user_name, self.pwd)
        self.client.will_set(f'WILL_{self.client_id}', 'Client Dead')

    def setup_post_con_params(self):
        for topic in self.sub_cbak_fn:
            Log.logger.info(f'{self.name}: Post Connection Subscribing: {topic}')
            self.client.subscribe(topic)
            self.client.message_callback_add(topic, self.sub_cbak_fn[topic])
        while len(self.pub_msg_queue) > 0:
            pub_msg = self.pub_msg_queue.popleft()
            try:
                self.client.publish(pub_msg[0], pub_msg[1])
                Log.logger.info(f'{self.name}: Post Connection - Publish : {pub_msg[0]}')
            except:
                self.pub_msg_queue.appendleft(pub_msg)
                Log.logger.error(
                    f'{self.name}: *** Post Connection {self.broker_ip}  Pub Failed {sys.exc_info()[0]} ***')
                traceback.print_exc(file=sys.stdout)
                break

    def start_reconnect_th(self):
        self.thread_started = True
        self.thread_quit = False
        Log.logger.warning(f'{self.name}: *** Starting reconnect Thread Broker: {self.broker_ip}  ***')
        self.client.loop_stop()
        if self.is_connected:
            self.client.disconnect()
            time.sleep(5)
            self.is_connected = False
        self.th = threading.Thread(target=self.reconnect, args=())
        self.th.start()

    def reconnect(self):
        try:
            self.client.reinitialise(self.client_id, clean_session=True, userdata=None)
            self.setup_pre_con_params()
        except:
            Log.logger.critical(f'{self.name}: ***** Unable to reinitialize  MQTT broker : {self.broker_ip}  *****')
        while not self.is_connected and not self.thread_quit:
            time.sleep(10)
            try:
                Log.logger.warning(f'{self.name}: ***** Trying to RECONNECT to  MQTT broker : {self.broker_ip}  *****')
                self.client.connect(self.broker_ip, self.broker_port, 60)
                self.client.loop_start()
                while not self.is_connected and not self.con_error and not self.thread_quit:
                    pass
                if self.is_connected:
                    Log.logger.warning(f'{self.name}: ***** RECONNECT MQTT broker : {self.broker_ip}  Success *****')
                    self.setup_post_con_params()
                    self.thread_started = False
            except:
                Log.logger.error(f'{self.name}: ***** Unable to RECONNECT to  MQTT broker : {self.broker_ip} '
                                 f'{sys.exc_info()[0]} *****')
        Log.logger.info(
            f'{self.name}: Exiting the reconnect thread  MQTT broker : {self.broker_ip} client_id: {self.client_id}')

    def disconnect(self):
        Log.logger.info(f'{self.name}: In disconnect fn loop stop')
        self.manual_discon = True
        self.client.loop_stop()
        if self.is_connected:
            Log.logger.info(f'{self.name}: In disconnect fn calling disconnect')
            self.client.disconnect()
            Log.logger.info(f'{self.name}: In disconnect fn After disconnect')
            time.sleep(5)
            self.is_connected = False
            Log.logger.info(f'{self.name}: Disconnected  MQTT broker : {self.broker_ip} client_id: {self.client_id}')
        else:
            if self.thread_started:
                # exit the thread
                self.thread_quit = True
            Log.logger.info(
                f'{self.name}: Already Disconnected  MQTT broker : {self.broker_ip} client_id: {self.client_id}')
        self.sub_cbak_fn = {}
        self.pub_msg_queue = deque()

    def on_con(self, client, user_data, flags, rc):
        if rc == 0:
            errtext = "Connection successful"
        elif rc == 1:
            errtext = "Connection refused: Unacceptable protocol version"
        elif rc == 2:
            errtext = "Connection refused: Identifier rejected"
        elif rc == 3:
            errtext = "Connection refused: Server unavailable"
        elif rc == 4:
            errtext = "Connection refused: Bad user name or password"
        elif rc == 5:
            errtext = "Connection refused: Not authorized"
        else:
            errtext = "Connection refused: Unknown reason"
        if rc == 0:
            Log.logger.info(f'{self.name}: Broker: {self.broker_ip} Connect Result: {errtext}')
            self.is_connected = True
            self.con_error = False
        else:
            Log.logger.error(f'{self.name}: Broker: {self.broker_ip} Connect Result: {errtext}')
            self.is_connected = False
            self.con_error = True

    def on_discon(self, client, user_data, rc):
        if not self.manual_discon:
            self.is_connected = False
            Log.logger.critical(
                f'{self.name}: ***** Unexpectedly DISCONNECTED - MQTT broker : {self.broker_ip} Retrying *****')
            if not self.thread_started:
                self.start_reconnect_th()
        else:
            self.manual_discon = False
            Log.logger.info(f'{self.name}: in on_discon fn Broker: {self.broker_ip} Manual Disconnection')

    def on_msg(self, in_client, user_data, message):
        Log.logger.info(f'{self.name}: Received Message from Broker: {self.broker_ip}')
        Log.logger.info(f'\n{self.name}: topic: {message.topic} \nmessage: {message.payload}\nQoS: {message.qos}'
                        f'\nUser data : {user_data}')

    def pub(self, topic, msg):
        self.pub_msg_queue.append((topic, msg))
        if self.is_connected:
            while len(self.pub_msg_queue) > 0:
                pub_msg = self.pub_msg_queue.popleft()
                try:
                    self.client.publish(pub_msg[0], pub_msg[1])
                    Log.logger.info(f'Publish : {pub_msg[0]}')
                except:
                    self.pub_msg_queue.appendleft(pub_msg)
                    break
        else:
            Log.logger.warning(f'{self.name}: Broker not connected - Queuing Publish : topic = {topic} Message = {msg}')

    def sub(self, topic, call_back_fn):
        if self.is_connected:
            self.client.subscribe(topic)
            self.client.message_callback_add(topic, call_back_fn)
            Log.logger.info(f'{self.name}: Subscribe : topic = {topic} Success')
        else:
            Log.logger.info(f'{self.name}: Broker not connected - Unable to Subscribe : topic = {topic}')
        self.sub_cbak_fn[topic] = call_back_fn


class TestSub:

    def test_sub_fn(self, in_client, user_data, message):
        Log.logger.info(f'{self.name}: In Test Sub Function')


if __name__ == '__main__':
    my_log = Log()
    mqtt_client = MqttClient("127.0.0.1", 1883, "Simulator", '', '', 'Name1')
    mqtt_client.connect()
    tst_sub = TestSub()
    mqtt_client.pub("Test", "In Main")
    mqtt_client.sub("sub", tst_sub.test_sub_fn)
    mqtt_client.disconnect()
    mqtt_client = MqttClient("127.0.0.1", 1883, "Simulator", '', '', 'Name2')
    mqtt_client.connect()
    mqtt_client.pub("Test123", "In Main 123")
    counter = 0
    while True:
        time.sleep(10)
        mqtt_client.pub(f'Test-{counter}', f'In Loop : {counter}')
        # print('waiting')
        counter = counter + 1
    sys.exit(0)
