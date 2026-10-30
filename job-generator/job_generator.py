import json
import os
import random
import datetime
import time

import paho.mqtt.client as mqtt

job_id = 0
job_types = ["pla", "petg", "abs"]

def generate_job():
    global job_id
    job_id += 1
    return {
        "job_id": job_id,
        "job_type": random.choice(job_types),
        "release_time": datetime.datetime.now().isoformat(),
        "deadline": (datetime.datetime.now() + datetime.timedelta(hours=2)).isoformat(),
    }

def publish_jobs(count, wait_time):
    for _ in range(count):
        job = generate_job()
        print(json.dumps(job, indent=4), flush=True)
        mqttc.publish("jobs", json.dumps(job))
    
    time.sleep(wait_time)

#def on_connect(client, userdata, flags, reason_code, properties):
#    print(f"Connected with result code {reason_code}")
#    client.subscribe("$SYS/#")

mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
#mqttc.on_connect = on_connect
mqtt_host = os.getenv("MQTT_BROKER_HOST", "localhost")
mqtt_port = int(os.getenv("MQTT_BROKER_PORT", "1883"))

while True:
    try:
        mqttc.connect(mqtt_host, mqtt_port, 60)
        break
    except ConnectionRefusedError:
        print("MQTT broker is not ready; retrying in 2 seconds.", flush=True)
        time.sleep(2)

state = "normal"

while True:

    print(state)
    match state:
        case "normal":
            wait_time = random.randint(1, 5)
            publish_jobs(1, wait_time)
            if random.random() < 0.2:
                state = "burst"
            elif random.random() < 0.4:
                state = "mass_order"
        case "burst":
            wait_time = random.randint(1, 5) / 5
            publish_jobs(random.randint(1, 3), wait_time)
            if random.random() < 0.05:
                state = "normal"
        case "mass_order":
            wait_time = random.randint(1, 5)
            publish_jobs(random.randint(7, 14), wait_time)
            state = "normal"

