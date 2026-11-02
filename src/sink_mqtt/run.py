import json
import sys
import datetime

JOBS_TOPIC_WILDCARD = "jobs/#"  # Deckt mehrere Generatoren ab (z.B. generator1/jobs, generator2/jobs oder einfach jobs/#)
TICK_TOPIC = "tickgen/tick"
STATS_TOPIC = "sink/stats"

# Zustand der Sink
active_jobs = set()  # Speichert eindeutige Job-Schlüssel (Topic + Job-ID)
completed_jobs_count = 0  # Aktuell noch 0, da Jobs noch nicht abgearbeitet werden
new_jobs_count = 0  # Zähler für neu eingetroffene Jobs im aktuellen Intervall


def on_message_job(client, userdata, msg):
    global new_jobs_count, active_jobs
    try:
        job = json.loads(msg.payload.decode("utf-8"))
        job_id = job.get("job_id")

        # Eindeutiger Schlüssel, falls mehrere Generatoren gleiche IDs verwenden
        job_key = (msg.topic, job_id)

        if job_key not in active_jobs:
            active_jobs.add(job_key)
            new_jobs_count += 1
    except Exception as e:
        print(f"Fehler beim Parsen des Jobs: {e}", flush=True)


def on_message_tick(client, userdata, msg):
    global new_jobs_count, active_jobs, completed_jobs_count
    timestamp = msg.payload.decode("utf-8")

    # Aktuelle Auftragsrate (z. B. Anzahl neuer Jobs in diesem Tick-Intervall)
    current_order_rate = new_jobs_count

    stats = {
        "timestamp": timestamp,
        "new_jobs_per_time_unit": new_jobs_count,
        "active_jobs_count": len(active_jobs),
        "completed_jobs_count": completed_jobs_count,
        "current_order_rate": current_order_rate
    }

    # Statistiken ausgeben und per MQTT veröffentlichen
    print(json.dumps(stats, indent=4), flush=True)
    client.publish(STATS_TOPIC, json.dumps(stats))

    # Zähler für das nächste Intervall zurücksetzen
    new_jobs_count = 0


def main():
    # MQTTWrapper entsprechend der bestehenden Architektur
    mqtt = MQTTWrapper('mqttbroker', 1883, name='job_sink')

    # Abonnieren der Topics
    mqtt.subscribe(JOBS_TOPIC_WILDCARD)
    mqtt.subscribe(TICK_TOPIC)

    # Callbacks registrieren
    mqtt.subscribe_with_callback(JOBS_TOPIC_WILDCARD, on_message_job)
    mqtt.subscribe_with_callback(TICK_TOPIC, on_message_tick)

    try:
        # MQTT-Schleife starten
        mqtt.loop_forever()
    except (KeyboardInterrupt, SystemExit):
        mqtt.stop()
        sys.exit("KeyboardInterrupt -- shutdown gracefully.")


if __name__ == '__main__':
    main()