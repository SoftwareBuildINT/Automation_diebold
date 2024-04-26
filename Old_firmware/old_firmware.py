import pandas as pd
import paho.mqtt.client as mqtt
import schedule
import time
import random
import json
import os
from datetime import datetime, timedelta, timezone

def load_json_file(file_name):
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the full file path
    file_path = os.path.join(script_dir, file_name)
    # Open and read the JSON file
    with open(file_path, 'r') as file:
        # Load JSON data from the file
        return json.load(file)

# MQTT Broker details
mqtt_broker = "15.207.28.229"
mqtt_port = 1883
mqtt_username = "buildint"
mqtt_password = "buildint"
client_id = f'python-mqtt-{random.randint(0, 1000)}'

def send_mqtt_message(mac_id, turn_off, client):
    
    if turn_off:
        mqtt_message = load_json_file('turn_off.json')
    else:
        mqtt_message = load_json_file('turn_on.json')

        
    # print(mqtt_message)
    # Connect to MQTT broker
    # client = mqtt.Client(client_id)
    # client.username_pw_set(mqtt_username, mqtt_password)
    # client.connect(mqtt_broker, mqtt_port)

    # Publish message to topic
    topic = f"settings/{mac_id}"
    client.publish(topic, json.dumps(mqtt_message))
    # client.publish(topic, f"GETPL")

    # Disconnect from MQTT broker
    # client.disconnect()

def read_and_send_messages(csv_file, turn_off, client):
    # Read CSV file
    df = pd.read_csv(csv_file)

    #Print the start time of the job
    utc_now = datetime.now(timezone.utc)
    # Convert UTC time to Indian Standard Time (IST) by adding 5 hours and 30 minutes
    ist_now = utc_now + timedelta(hours=5, minutes=30)
    ist_now = ist_now.strftime(f"%Y-%m-%d %H:%M:%S")
    print(f"Message sent at: {ist_now} IST time")

    # Iterate over each row
    for index, row in df.iterrows():
        mac_id = row.iloc[0]  # Assuming MAC ID is in the first column
        print(mac_id)
        send_mqtt_message(mac_id, turn_off, client)

def job():
    global response_topic  # Ensure you have declared this global variable
    # Connect to MQTT broker
    client = mqtt.Client(client_id)
    client.username_pw_set(mqtt_username, mqtt_password)

    client.connect(mqtt_broker, mqtt_port)
    client.loop_start() 

    #Print the start time of the job
    utc_now = datetime.now(timezone.utc)
    # Convert UTC time to Indian Standard Time (IST) by adding 5 hours and 30 minutes
    ist_now = utc_now + timedelta(hours=5, minutes=30)
    ist_now = ist_now.strftime(f"%Y-%m-%d %H:%M:%S")
    print(f"Automation scheduling started at: {ist_now} IST time")

    schedule.every(1).hour.do(read_and_send_messages, 'ALL_SITES(8_TO_10).csv', False, client)

    # Schedule task to turn on AC daily at 8 AM and 9:30 PM UTC+05:30
    # schedule.every().day.at("02:30").do(read_and_send_messages, 'ALL_SITES(8_TO_10).csv', False, client)
    # schedule.every().day.at("16:30").do(read_and_send_messages, 'ALL_SITES(8_TO_10).csv', True, client)

    # Schedule hourly execution to turn on AC after 08:00 am till 10:00 pm UTC+05:30 for ALL_SITES
    # for hour in range(3, 16):
    #     schedule.every().day.at(f"{hour:02}:30").do(read_and_send_messages, 'ALL_SITES(8_TO_10).csv', False, client)

    # # Schedule hourly execution to turn off AC after 20:00 for ALL_SITES
    # for hour in range(17, 24):
    #     schedule.every().day.at(f"{hour:02}:30").do(read_and_send_messages, 'ALL_SITES(8_TO_10).csv', True, client)
    # for hour in range(0, 2):
    #     schedule.every().day.at(f"{hour:02}:30").do(read_and_send_messages, 'ALL_SITES(8_TO_10).csv', True, client)

    while True:
        schedule.run_pending()
        time.sleep(5)  # Check every 5 seconds

if __name__ == "__main__":
    job()
