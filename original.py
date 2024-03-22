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

def send_mqtt_message(mac_id):
    # MQTT message payload
    mqtt_message = {
        "account": "2",
        "logicmode": "temp",
        "mintemp": "-2",
        "maxtemp": "-1",
        "motionenabled": False,
        "r0s": True,
        "r1s": True,
        "r0e": True,
        "r1e": True,
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

def read_and_send_messages():
    # Read CSV file
    df = pd.read_csv('ALL_SITES(8_TO_8).csv')  # Update with your CSV file path

    # Iterate over each row
    for index, row in df.iterrows():
        mac_id = row.iloc[0]  # Assuming MAC ID is in the first column
        print(mac_id)
        send_mqtt_message(mac_id)

def job():
    # Schedule task to run daily at 8 AM
    schedule.every().day.at("10:20").do(read_and_send_messages)

    while True:
        schedule.run_pending()
        time.sleep(5)  # Check every minute

if __name__ == "__main__":
    job()