from flask import Flask, render_template, send_file
import pandas as pd
import paho.mqtt.client as mqtt
import schedule
import time
import random
import mysql.connector
from datetime import datetime, timedelta, timezone

app = Flask(__name__)

# # MySQL database connection details
# mysql_host = '15.206.230.32'
# mysql_user = 'iems_admin'
# mysql_password = 'iemsadminPa$$word'
# mysql_database = 'db_iems'

# MySQL database connection details
mysql_host = '35.154.28.201'
mysql_user = 'euronet'
mysql_password = f'IUBif872878Gbjv$ks&%d9uhih757'
mysql_database = 'db_iems'

# Establish MySQL connection
mysql_connection = mysql.connector.connect(
    host=mysql_host,
    user=mysql_user,
    password=mysql_password,
    database=mysql_database
)
mysql_cursor = mysql_connection.cursor()

# MQTT Broker details
mqtt_broker = "15.206.230.32"
mqtt_port = 1883
mqtt_username = "mqtt_buildint"
mqtt_password = "mqtt_buildint_$$2023"
client_id = f'python-mqtt-{random.randint(0, 1000)}'

# Global variable to store sent DIDs
all_DIDs = []
in_manual_mode = []
response_received = []
switched_to_auto = []
excluded_DIDs = ['P1DCMU53']


def query_data():
    try:
        response_received.clear()
        all_DIDs.clear()
        in_manual_mode.clear()

        # Execute the SQL query to get loc_id from mst_location table
        mysql_cursor.execute("SELECT dev_id FROM mst_device WHERE org_id = 10;")

        # Fetch all loc_ids
        dev_ids = mysql_cursor.fetchall()

        # Process the fetched loc_ids
        for dev_id in dev_ids:
            all_DIDs.append(dev_id[0])

    except mysql.connector.Error as error:
        print("Error querying data from MySQL database:", error)


def on_message(client, userdata, message):

    payload_parts = message.payload.decode("utf-8").split(",")

    if len(payload_parts) >= 3:
        # Callback function when a message is received
        response_DID = message.payload.decode("utf-8").split(",")[0]  # Extracting DID from the message
        command = message.payload.decode("utf-8").split(",")[2]

        if command == "$GRES" and response_DID in all_DIDs:
            print("Received response:", response_DID)
            print("Command:", command)
            relay_status = message.payload.decode("utf-8").split(",")[3]
            print("RS", relay_status)
            if len(relay_status) < 1:
                in_manual_mode.append(response_DID)
                all_DIDs.remove(response_DID)
                print("Remaining to check:", len(all_DIDs))
            elif relay_status[-1] == "0" or "":
                print(f"{response_DID} is in manual mode {relay_status}")
                in_manual_mode.append(response_DID)
                all_DIDs.remove(response_DID)
                print("Remaining to check:", len(all_DIDs))
            elif relay_status[-1] == "1":
                all_DIDs.remove(response_DID)
                print("Remaining to check:", len(all_DIDs))

        elif response_DID in in_manual_mode and command == "$SRMK":
            print("Received response:", response_DID)
            print("Command:", command)
            # Store DID whose response has been recieved 
            response_received.append(response_DID)  
            print("$SRMK response recieved")
            # Remove DID from the array if response received
            in_manual_mode.remove(response_DID)
            print("Still in manual mode:", len(in_manual_mode))
            print("Sent SRMK:", response_received)

        elif response_DID in response_received and command == "$SREL":
            print("Received response:", response_DID)
            print("Command:", command)
            # Store DID whose response has been recieved
            switched_to_auto.append(response_DID)
            print("$SREL response recieved")
            # Remove DID from the array if response received
            response_received.remove(response_DID)
            print("Remaining to send SREL:", len(response_received))
            print("Changed to auto mode:", switched_to_auto)
    else:
        print("Received payload doesn't match standard.", payload_parts)

def check_manual(DID, client):
    # MQTT message payload for Automatic operation
    mqtt_message = f"{DID}$GRES,"

    # Publish message to topic  
    topic = f"Settings"
    client.publish(topic, str(mqtt_message))

def set_mask(DID, client):
    # MQTT message payload for Automatic operation
    mqtt_message = f"{DID}$SRMK11111111,"
    # print(mqtt_message)

    # Publish message to topic  
    topic = f"Settings"
    client.publish(topic, str(mqtt_message))

def set_relay(DID, client):
    # MQTT message payload for Automatic operation
    mqtt_message = f"{DID}$SREL11110000,"
    # print(mqtt_message)

    # Publish message to topic  
    topic = f"Settings"
    client.publish(topic, str(mqtt_message))

def read_and_send_status_command(client):
    global all_DIDs
    # Subscribe to Response topic
    client.subscribe("Response")

    print(f"Checking if manual, count: {len(all_DIDs)}")

    # Check if the array is empty
    # if not all_DIDs:
    #     call_query(client)

    # Iterate over each row
    for DID in all_DIDs:
        # print(DID)
        check_manual(DID, client)
        # sent_DIDs.append(DID)  # Store sent DIDs

def send_mask_messages(client):
    global in_manual_mode

    # Subscribe to Response topic
    client.subscribe("Response")

    print("Remaining to send SRMK:", len(in_manual_mode))

    for DID in in_manual_mode:
        # print(DID)
        set_mask(DID, client)

def send_relay_messages(client):
    global response_received

    # Subscribe to Response topic
    client.subscribe("Response")

    print("Remaining to send SREL:", len(response_received))

    for DID in response_received:
        # print(DID)
        set_relay(DID, client)

def remove_excluded():
    global all_DIDs
    global excluded_DIDs

    for DID in all_DIDs:
        if DID in excluded_DIDs:
            print(f"Excluding {DID} from the list")
            all_DIDs.remove(DID)

def call_query(client):
    query_data()
    remove_excluded()
    print("Total dids:", all_DIDs)
    read_and_send_status_command(client)
    send_mask_messages(client)
    program_status()

def program_status():
    print("Remaining to check", all_DIDs)
    print("In manual mode:", in_manual_mode)
    print("Recieved SRMK:", response_received)
    print("Switched to auto mode:", switched_to_auto)
   
def job():
    # Connect to MQTT broker
    client = mqtt.Client(client_id)
    client.username_pw_set(mqtt_username, mqtt_password)

    client.on_message = on_message

    client.connect(mqtt_broker, mqtt_port)
    client.loop_start()

    # Start time of the job
    utc_now = datetime.now(timezone.utc)
    ist_now = utc_now + timedelta(hours=5, minutes=30)
    ist_now = ist_now.strftime(f"%Y-%m-%d %H:%M:%S")
    print(f"Automation scheduling started at: {ist_now} IST time")

    call_query(client)

    # read_and_send_status_command(client)
    # print("Total dids:", len(all_DIDs))
    # send_mask_messages(client)

    schedule.every(1).hours.do(call_query, client)
    schedule.every(5).minutes.do(read_and_send_status_command, client)
    schedule.every(5).minutes.do(send_mask_messages, client)
    schedule.every(5).minutes.do(send_relay_messages, client)
    schedule.every(5).minutes.do(program_status)

    # schedule.every(1).minute.do(call_query, client)
    # schedule.every(30).seconds.do(read_and_send_status_command, client)
    # schedule.every(30).seconds.do(send_mask_messages, client)
    # schedule.every(30).seconds.do(send_relay_messages, client)
    # schedule.every(2).minutes.do(program_status)

    while True:
        schedule.run_pending()
        # Check every 1 second
        time.sleep(1)

if __name__ == "__main__":
    job()