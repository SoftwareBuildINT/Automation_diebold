import pandas as pd
import paho.mqtt.client as mqtt
import schedule
import time
import random
import mysql.connector
from datetime import datetime, timedelta, timezone

# MQTT Broker details
mqtt_broker = "15.206.230.32"
mqtt_port = 1883
mqtt_username = "mqtt_buildint"
mqtt_password = "mqtt_buildint_$$2023"
client_id = f'python-mqtt-{random.randint(0, 1000)}'

# MySQL connection parameters
config = {
    'user': 'iems_admin',
    'password': 'your_password',
    'host': '15.206.230.32',  # Or your host
    'database': 'db_iems',
    'raise_on_warnings': True
}

def send_mqtt_message(mac_id, turn_off=False):
    # MQTT message payload for Automatic operation
    mqtt_message = f"$SRMK11111111"
        
    # Connect to MQTT broker
    client = mqtt.Client(client_id)
    client.username_pw_set(mqtt_username, mqtt_password)
    client.connect(mqtt_broker, mqtt_port)

    # Publish message to topic
    topic = f"settings/{mac_id}"
    client.publish(topic, str(mqtt_message))

    # Disconnect from MQTT broker
    client.disconnect()

def read_and_send_messages(csv_file, turn_off):
    # Read CSV file
    df = pd.read_csv(csv_file)

    # Convert UTC time to Indian Standard Time (IST) by adding 5 hours and 30 minutes
    utc_now = datetime.now(timezone.utc)
    ist_now = utc_now + timedelta(hours=5, minutes=30)
    ist_now = ist_now.strftime(f"%Y-%m-%d %H:%M:%S")
    print(f"Message sent at: {ist_now} IST time")

    # Iterate over each row
    for index, row in df.iterrows():
        mac_id = row.iloc[0]  # Assuming MAC ID is in the first column
        print(mac_id)
        send_mqtt_message(mac_id, turn_off)

def job():
    #Print the start time of the job
    utc_now = datetime.now(timezone.utc)
    # Convert UTC time to Indian Standard Time (IST) by adding 5 hours and 30 minutes
    ist_now = utc_now + timedelta(hours=5, minutes=30)
    ist_now = ist_now.strftime(f"%Y-%m-%d %H:%M:%S")
    print(f"Automation scheduling started at: {ist_now} IST time")

    # Schedule task to turn on AC daily at 8 AM and 9:30 PM UTC+05:30
    schedule.every().day.at("02:30").do(read_and_send_messages, 'All_sites_new_firmware.csv', turn_off=False)
    schedule.every().day.at("16:30").do(read_and_send_messages, 'All_sites_new_firmware.csv', turn_off=True)

    # Schedule hourly execution to turn on AC after 08:00 am till 10:00 pm UTC+05:30 for ALL_SITES
    for hour in range(3, 16):
        schedule.every().day.at(f"{hour:02}:30").do(read_and_send_messages, 'ALL_SITES(8_TO_10).csv', turn_off=False)

    # Schedule hourly execution to turn off AC after 20:00 for ALL_SITES
    for hour in range(17, 24):
        schedule.every().day.at(f"{hour:02}:30").do(read_and_send_messages, 'ALL_SITES(8_TO_10).csv', turn_off=True)
    for hour in range(0, 2):
        schedule.every().day.at(f"{hour:02}:30").do(read_and_send_messages, 'ALL_SITES(8_TO_10).csv', turn_off=True)

    while True:
        schedule.run_pending()
        time.sleep(5)  # Check every 5 seconds

if __name__ == "__main__":
    job()