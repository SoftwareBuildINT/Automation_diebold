import pandas as pd
import paho.mqtt.client as mqtt
import schedule
import time
import random

# MQTT Broker details
mqtt_broker = "15.207.28.229"
mqtt_port = 1883
mqtt_username = "buildint"
mqtt_password = "buildint"
client_id = f'python-mqtt-{random.randint(0, 1000)}'

def send_mqtt_message(mac_id, turn_off=False):
    if turn_off:
        # MQTT message payload for night time
        mqtt_message = {
            "r0s": "false", 
            "r1s": "false",
            "r0e": "false",
            "r1e": "false",
        }
    else:
        # MQTT message payload for normal operation
        mqtt_message = {
            "account": "2",
            "logicmode": "temp",
            "mintemp": "-2",
            "maxtemp": "-1",
            "motionenabled": "false",
            "r0s": "true",
            "r1s": "true",
            "r0e": "true",
            "r1e": "true",
            "acontime": 7200,
            "acofftime": 600
        }
        
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

    # Iterate over each row
    for index, row in df.iterrows():
        mac_id = row.iloc[0]  # Assuming MAC ID is in the first column
        print(mac_id)
        send_mqtt_message(mac_id, turn_off)

def job():
    # Schedule task to turn on AC daily at 8 AM and 9:30 PM UTC+05:30
    schedule.every().day.at("02:30").do(read_and_send_messages, 'ALL_SITES(8_TO_8).csv', turn_off=False)
    schedule.every().day.at("14:30").do(read_and_send_messages, 'ALL_SITES(8_TO_8).csv', turn_off=True)
    schedule.every().day.at("02:30").do(read_and_send_messages, 'Hyderbad(8_TO_9,30).csv', turn_off=False)
    schedule.every().day.at("16:00").do(read_and_send_messages, 'Hyderbad(8_TO_9,30).csv', turn_off=True)

    # Schedule hourly execution to turn on AC after 08:00 for ALL_SITES
    for hour in range(8, 20):
        schedule.every().day.at(f"{hour:02}:00").do(read_and_send_messages, 'ALL_SITES(8_TO_8).csv', turn_off=False)
        schedule.every().day.at(f"{hour:02}:00").do(read_and_send_messages, 'Hyderbad(8_TO_9,30).csv', turn_off=False)

    # Schedule hourly execution to turn off AC after 20:00 for ALL_SITES
    for hour in range(15, 24):
        schedule.every().day.at(f"{hour:02}:00").do(read_and_send_messages, 'ALL_SITES(8_TO_8).csv', turn_off=True)
    for hour in range(0, 3):
        schedule.every().day.at(f"{hour:02}:00").do(read_and_send_messages, 'ALL_SITES(8_TO_8).csv', turn_off=True)

    # Schedule hourly execution to turn off AC after 21:30 for Hyderbad
    for hour in range(17, 24):
        schedule.every().day.at(f"{hour:02}:00").do(read_and_send_messages, 'Hyderbad(8_TO_9,30).csv', turn_off=True)
    for hour in range(0, 3):
        schedule.every().day.at(f"{hour:02}:00").do(read_and_send_messages, 'Hyderbad(8_TO_9,30).csv', turn_off=True)

    while True:
        schedule.run_pending()
        time.sleep(5)  # Check every 5 seconds

if __name__ == "__main__":
    job()
