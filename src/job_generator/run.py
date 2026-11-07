import json
import random
import datetime
import sys
import os

from mqtt.mqtt_wrapper import MQTTWrapper

TICK_TOPIC = "tickgen/tick"
JOBS_TOPIC = "generator/jobs"
STATUS_TOPIC = "generator/status"
STATE_TOPIC = "fuxa/generator/state"

job_id = 0
job_types = ["pla", "petg", "abs"]
state = "normal"
tick_counter = 0
job_randomness = 0
client_name = os.environ.get("GENERATOR_NAME", "gen_no_name")

PROBABILITY_NORMAL = 0.05
PROBABILITY_BURST = 0.05
PROBABILITY_MASS_ORDER = 0.05

def generate_job(timestamp):
    global job_id
    job_id += 1
    release_time = datetime.datetime.fromisoformat(timestamp)
    unique_job_id = f"{client_name}_{job_id}"
    
    return {
        "job_id": unique_job_id,
        "job_type": random.choice(job_types),
        "release_time": timestamp,
        "deadline": (release_time + datetime.timedelta(hours=2)).isoformat(),
    }


def publish_jobs(count, timestamp, client):
    for _ in range(count):
        job = generate_job(timestamp)
        print(json.dumps(job, indent=4), flush=True)
        client.publish(JOBS_TOPIC, json.dumps(job))


def on_message_state(client, userdata, msg):
    global state
    new_state = msg.payload.decode("utf-8")
    if new_state in ["normal", "burst", "max"]:
        state = new_state


def on_message_tick(client, userdata, msg):
    global state
    global tick_counter
    global job_randomness
    tick_counter += 1
    timestamp = msg.payload.decode("utf-8")

    match state:
        case "normal":
            if (tick_counter % 15) == 0:
                job_randomness = random.randint(-3, 3)
                publish_jobs(random.randint(0, 1), timestamp, client)
        case "burst":
            if (tick_counter % 3) == 0:
                job_randomness = random.randint(-2, 2)
                publish_jobs(random.randint(1, 3), timestamp, client)
        case "max":
            if (tick_counter % 2) == 0:
                job_randomness = random.randint(-1, 1)
                publish_jobs(random.randint(2, 4), timestamp, client)


def main():
    mqtt = MQTTWrapper('mqttbroker', 1883, name=client_name)
    mqtt.subscribe(TICK_TOPIC)
    mqtt.subscribe(STATE_TOPIC)
    mqtt.subscribe_with_callback(TICK_TOPIC, on_message_tick)
    mqtt.subscribe_with_callback(STATE_TOPIC, on_message_state)
    try:
        # Start the MQTT loop to process incoming and outgoing messages
        mqtt.loop_forever()
    except (KeyboardInterrupt, SystemExit):
        # Gracefully stop the MQTT client and exit the program on interrupt
        mqtt.stop()
        sys.exit("KeyboardInterrupt -- shutdown gracefully.")


if __name__ == '__main__':
    main()