from flask import Flask, render_template, send_file
import pandas as pd
import paho.mqtt.client as mqtt
import schedule
import time
import random
import mysql.connector
from datetime import datetime, timedelta, timezone

app = Flask(__name__)

# MySQL database connection details
mysql_host = '15.206.230.32'
mysql_user = 'iems_admin'
mysql_password = 'iemsadminPa$$word'
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
response_received = []
all_DIDs = []
in_manual_mode = []


def query_data():
    try:

        response_received.clear()
        all_DIDs.clear()
        in_manual_mode.clear()

        # Execute the SQL query to get loc_id from mst_location table
        mysql_cursor.execute("SELECT loc_id FROM mst_location where org_id = 10")

        # Fetch all loc_ids
        loc_rows = mysql_cursor.fetchall()

        # Process the fetched loc_ids
        for loc_row in loc_rows:
            loc_id = loc_row[0]  # Extract loc_id from the row

            # Execute the SQL query to get dev_id from mst_device table based on loc_id
            mysql_cursor.execute(f"SELECT dev_id FROM mst_device WHERE loc_id = {loc_id} AND (name = 'ATM' OR name = 'iATM')")

            # Fetch all dev_ids for the current loc_id
            dev_rows = mysql_cursor.fetchall()

            # Process the fetched dev_ids
            for dev_row in dev_rows:
                dev_id = dev_row[0]  # Extract dev_id from the row
                # print(f"Location ID: {loc_id}, Device ID: {dev_id}")
                all_DIDs.append(dev_id)

    except mysql.connector.Error as error:
        print("Error querying data from MySQL database:", error)

# Call the function to execute the query
# query_data()

# print(all_DIDs)
# print("Total dids:", len(all_DIDs))

def on_message(client, userdata, message):
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
        response_received.append(response_DID)  # Store DID which has recieved the response
        print("$SRMK response recieved")
        # Remove DID from the array if response received
        in_manual_mode.remove(response_DID)
        print("Still in manual mode:", len(in_manual_mode))
        print("Changed to auto mode:", response_received)

def check_manual(DID, client):
    # MQTT message payload for Automatic operation
    mqtt_message = f"{DID}$GRES"

    # Publish message to topic  
    topic = f"Settings"
    client.publish(topic, str(mqtt_message))
        
def send_auto_message(DID, client):
    # MQTT message payload for Automatic operation
    mqtt_message1 = f"{DID}$SRMK11111111"
    mqtt_message2 = f"{DID}$SREL11110000"
    # print(mqtt_message1)
    # print(mqtt_message2)

    # Publish message to topic  
    topic = f"Settings"
    client.publish(topic, str(mqtt_message1))
    client.publish(topic, str(mqtt_message2))

def read_and_send_mask_messages(client):
    global all_DIDs
    # Subscribe to Response topic
    client.subscribe("Response")

    print(f"Checking if manual, count: {len(all_DIDs)}")

    # Check if the array is empty
    if not all_DIDs:
        call_query(client)

    # Iterate over each row
    for DID in all_DIDs:
        # print(DID)
        check_manual(DID, client)
        # sent_DIDs.append(DID)  # Store sent DIDs

def call_query(client):
    query_data()
    print("Total dids:", len(all_DIDs))
    read_and_send_mask_messages(client)
    send_mask_messages(client)

def job():
    # Connect to MQTT broker
    client = mqtt.Client(client_id)
    client.username_pw_set(mqtt_username, mqtt_password)

    client.on_message = on_message  # Set callback function for message received

    client.connect(mqtt_broker, mqtt_port)
    client.loop_start()

    # Start time of the job
    utc_now = datetime.now(timezone.utc)
    ist_now = utc_now + timedelta(hours=5, minutes=30)
    ist_now = ist_now.strftime(f"%Y-%m-%d %H:%M:%S")
    print(f"Automation scheduling started at: {ist_now} IST time")

    # Schedule task to read and send messages from the CSV file
    # read_and_send_mask_messages(client)
    
    call_query(client)
    # print("Total dids:", len(all_DIDs))

    # read_and_send_mask_messages(client)

    # send_mask_messages(client)


    schedule.every(2).hours.do(call_query, client)
    
    schedule.every(38).minutes.do(read_and_send_mask_messages, client)

    schedule.every(38).minutes.do(send_mask_messages, client)

    schedule.every(45).minutes.do(program_status)

    # schedule.every(1).minute.do(call_query, client)
    
    # schedule.every(30).seconds.do(read_and_send_mask_messages, client)

    # schedule.every(30).seconds.do(send_mask_messages, client)

    # schedule.every(45).minutes.do(program_status)

    while True:
        schedule.run_pending()
        # Check every 1 second
        time.sleep(1)

def program_status():
    print("Remaining to check", all_DIDs)
    print("In manual mode:", in_manual_mode)
    print("Switched to auto mode:", response_received)

def send_mask_messages(client):
    global in_manual_mode

    # Subscribe to Response topic
    client.subscribe("Response")

    print("Remaining to send SRMK:", len(in_manual_mode))

    for DID in in_manual_mode:
        # print(DID)
        send_auto_message(DID, client)

if __name__ == "__main__":
    job()