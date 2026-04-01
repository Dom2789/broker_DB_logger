import sqlite3

import paho.mqtt.client as mqtt
from time import strftime
from _datetime import datetime
from .paramters import Parameters
import sqlite3 as sql
import logging

class MQTT:
    def __init__(self, parameters:Parameters):
        self.path_DB = parameters.path + parameters.file_name
        self.broker_IP = parameters.broker_IP
        self.broker_port = parameters.broker_port
        self.topic = parameters.topic
        self.conn = self.init_db()
        self._logger = logging.getLogger(parameters.file_name)


    def init_db(self):
        conn = sql.connect(self.path_DB, check_same_thread=False)
        conn.execute("""CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                topic TEXT,
                payload TEXT)""")
        conn.commit()
        return conn

    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(self, client, userdata, flags, rc, properties=None):
        print("Connected with result code " + str(rc))
        self._logger.info("Connected with result code " + str(rc))
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        client.subscribe(self.topic)

    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        message = msg.payload.decode()
        self.db_output(msg.topic, message)
        print(f"[{msg.topic}] {message}")


    # subscribe method
    def subscribe(self):
        client = mqtt.Client(protocol=mqtt.MQTTv5, transport="tcp")
        client.enable_logger()
        client.on_connect = self.on_connect
        client.on_message = self.on_message

        client.connect(self.broker_IP, self.broker_port, 60, bind_address="0.0.0.0")

        print(f"subcribed to topic: {self.topic}")
        self._logger.info(f"Connected to broker: {self.broker_IP}:{self.broker_port}, subscribed to topic: {self.topic}")

        client.loop_start()  # non-blocking, runs in background thread
        return client        # caller holds reference to keep it alive


    def txt_file_output(self, topic, message):
        header, tail = self.file_name.split(".")
        name_logfile = f"{header}_{strftime('%Y%m%d')}.{tail}"
        path = self.path + "/" + name_logfile
        with open(path, "a") as file:
            file.write(f"[{topic}] {message}\n")


    def db_output(self, topic, message):
        try:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO messages (timestamp, topic, payload) VALUES (?, ?, ?)",
                    (datetime.now().isoformat(),topic, message)
                )
        except sqlite3.Error as e:
            self._logger.error(f"Failed to insert [{topic}][{message}] into database! {e})")