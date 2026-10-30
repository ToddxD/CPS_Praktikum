import json
import random
import datetime
import sys

from mqtt.mqtt_wrapper import MQTTWrapper

job_id = 0
job_types = ["pla", "petg", "abs"]
TICK_TOPIC = "tickgen/tick"
JOBS_TOPIC = "jobs"
state = "normal"

def generate_job(timestamp):
    global job_id
    job_id += 1
    release_time = datetime.datetime.fromisoformat(timestamp)
    return {
        "job_id": job_id,
        "job_type": random.choice(job_types),
        "release_time": timestamp,
        "deadline": (release_time + datetime.timedelta(hours=2)).isoformat(),
    }

def publish_jobs(count, timestamp, client):
    for _ in range(count):
        job = generate_job(timestamp)
        print(json.dumps(job, indent=4), flush=True)
        client.publish(JOBS_TOPIC, json.dumps(job))


def on_message_tick(client, userdata, msg):
    global state
    timestamp = msg.payload.decode("utf-8")

    print(state, flush=True)
    match state:
        case "normal":
            publish_jobs(random.randint(0, 1), timestamp, client)
            if random.random() < 0.1:
                state = "burst"
            elif random.random() < 0.4:
                state = "mass_order"
        case "burst":
            publish_jobs(random.randint(1, 3), timestamp, client)
            if random.random() < 0.1:
                state = "normal"
        case "mass_order":
            publish_jobs(random.randint(7, 14), timestamp, client)
            state = "normal"


def main():
    mqtt = MQTTWrapper('mqttbroker', 1883, name='job_generator')
    mqtt.subscribe(TICK_TOPIC)
    mqtt.subscribe_with_callback(TICK_TOPIC, on_message_tick)
    try:
        # Start the MQTT loop to process incoming and outgoing messages
        mqtt.loop_forever()
    except (KeyboardInterrupt, SystemExit):
        # Gracefully stop the MQTT client and exit the program on interrupt
        mqtt.stop()
        sys.exit("KeyboardInterrupt -- shutdown gracefully.")


if __name__ == '__main__':
    main()