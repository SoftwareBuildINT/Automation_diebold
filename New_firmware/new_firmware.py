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
sent_DIDs = []
response_recieved = []
all_DIDs = []


def query_data():
    try:
        # Execute the SQL query to get loc_id from mst_location table
        mysql_cursor.execute("SELECT loc_id FROM mst_location")

        # Fetch all loc_ids
        loc_rows = mysql_cursor.fetchall()

        # Process the fetched loc_ids
        for loc_row in loc_rows:
            loc_id = loc_row[0]  # Extract loc_id from the row

            # Execute the SQL query to get dev_id from mst_device table based on loc_id
            mysql_cursor.execute(f"SELECT dev_id FROM mst_device WHERE loc_id = {loc_id}")

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
query_data()

print(all_DIDs)
print("Total dids:", len(all_DIDs))


def on_message(client, userdata, message):
    # Callback function when a message is received
    response_DID = message.payload.decode("utf-8").split(",")[0]  # Extracting DID from the message
    command = message.payload.decode("utf-8").split(",")[2]
    print("Received response:", response_DID)
    print("Command:", command)
    if response_DID in sent_DIDs and command == "$SRMK":
        response_recieved.append(response_DID)  # Store DID which has recieved the response
        print("$SRMK response recieved")
        # Remove DID from the array if response received
        sent_DIDs.remove(response_DID)
        print("Response remaining from $SRMK DIDs:", len(sent_DIDs))
    elif response_DID in response_recieved and command == "$SREL":
        print("$SREL response recieved")
        # Remove DID from the array if response received
        response_recieved.remove(response_DID)
        print("Response remaining from $SREL DIDs:", len(response_recieved))
        
def send_manual_message(DID, client):
    # MQTT message payload for Automatic operation
    mqtt_message = f"{DID}$SRMK00111110"

    # print(mqtt_message)

    # Publish message to topic  
    topic = f"Settings"
    client.publish(topic, str(mqtt_message))

def send_ac_message(DID, client):
    # MQTT message payload for Automatic operation
    mqtt_message = f"{DID}$SREL11111110"

    # print(mqtt_message)

    # Publish message to topic
    topic = f"Settings"
    client.publish(topic, str(mqtt_message))

def read_and_send_mask_messages(csv_file, client):
    global sent_DIDs
    sent_DIDs = []  # Initialize list to store sent DIDs

    # Read CSV file
    df = pd.read_csv(csv_file)

    # Subscribe to Response topic
    client.subscribe("Response")

    # Iterate over each row
    for index, row in df.iterrows():
        DID = row.iloc[0]  # Assuming DID is in the first column
        print(DID)
        send_manual_message(DID, client)
        sent_DIDs.append(DID)  # Store sent DIDs

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
    read_and_send_mask_messages('E:/Saujeet/Diebold/Automation_For_AC/New_firmware/manual_test.csv', client)

    # Schedule task to read and send messages from the CSV file every 2 minutes
    schedule.every(1).minutes.do(send_mask_messages, client)
    schedule.every(1).minutes.do(send_messages, client)

    while True:
        schedule.run_pending()
        # Check every 1 second
        time.sleep(1)

def send_mask_messages(client):
    global sent_DIDs
    # Iterate through the list of DIDs and send messages

    # Subscribe to Response topic
    client.subscribe("Response")

    print("Remaining to send SRMK:", len(sent_DIDs))

    for DID in sent_DIDs:
        print(DID)
        send_manual_message(DID, client)

def send_messages(client):
    global response_recieved
    # Iterate through the list of DIDs and send messages

    # Subscribe to Response topic
    client.subscribe("Response")

    print("Remaining to send SREL:", len(response_recieved))

    for DID in response_recieved:
        print(DID)
        send_ac_message(DID, client)

if __name__ == "__main__":
    job()