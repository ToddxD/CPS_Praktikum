import asyncio
import collections
import os
import random
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt
from asyncua import Client, ua


SERVER_URL = os.getenv("OPC_SERVER_URL", "opc.tcp://opcua-server:4840/haw/cps")
NS_URI = os.getenv("OPC_NS_URI", "http://haw-hamburg.de/cps")
# Ein Container kann mehrere Instanzen desselben Typs fahren: MACHINE_IDS="FDM_1_1,FDM_1_2"
MACHINE_IDS = [
    m.strip()
    for m in os.getenv("MACHINE_IDS", os.getenv("MACHINE_ID", "FDM_1_1")).split(",")
    if m.strip()
]
MACHINE_TYPE = int(os.getenv("MACHINE_TYPE", "1"))
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "0.1"))
MQTT_HOST = os.getenv("MQTT_HOST", "mqttbroker")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
TICK_TOPIC = "tickgen/tick"
# Fenster fuer die gleitende Auslastung: 120 Ticks a 30 s Simulationszeit = 60 min
UTILIZATION_WINDOW_TICKS = int(os.getenv("UTILIZATION_WINDOW_TICKS", "120"))

# Durchlaufzeit = gleitendes Mittel der letzten N abgeschlossenen Jobs (Sim-Zeit)
THROUGHPUT_WINDOW_JOBS = int(os.getenv("THROUGHPUT_WINDOW_JOBS", "20"))

# Zustand (OPC UA Knoten "Zustand")
IDLE, ASSIGNED, ACTIVE, WAITING_POWER, FAILED = 0, 1, 2, 3, 4

# Aufgabenblatt 2.1.2: PROFILES[maschine][jobtyp] = [(Power W, Dauer min), ...]
# Reihenfolge der Stufen: 0 = Preheating, 1 = Printing, 2 = Cooling
PROFILES: dict[int, dict[int, list[tuple[float, float]]]] = {
    1: {
        1: [(364, 2), (196, 50), (42, 6)],
        2: [(357, 2), (203, 62), (43, 6)],
        3: [(370, 6), (242, 50), (42, 10)],
        4: [(354, 2), (201, 28), (48, 6)],
    },
    2: {
        1: [(352, 12), (300, 24), (43, 12)],
        2: [(373, 10), (320, 26), (48, 14)],
        3: [(371, 14), (338, 22), (43, 14)],
        4: [(371, 10), (334, 12), (45, 12)],
    },
    3: {
        1: [(371, 8), (302, 34), (43, 12)],
        2: [(372, 8), (379, 38), (44, 12)],
        3: [(372, 8), (290, 32), (43, 12)],
        4: [(372, 8), (303, 30), (46, 12)],
    },
}


def _parse_sim_time(text: str) -> datetime:
    value = datetime.fromisoformat(text.strip().replace("Z", "+00:00"))
    return _as_utc(value)


def _as_utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


class FdmWorker:
    def __init__(
        self,
        endpoint: str,
        ns_uri: str,
        machine_id: str,
        machine_type: int,
        poll_interval: float = 0.1,
    ):
        self.endpoint = endpoint
        self.ns_uri = ns_uri
        self.machine_id = machine_id
        self.machine_type = machine_type if machine_type in PROFILES else 1
        self.poll_interval = poll_interval
        self.client = Client(self.endpoint)
        self.ns: int | None = None

        # Simulationszeit (kommt ueber MQTT vom tick_gen_mqtt)
        self.sim_now: datetime | None = None
        self.tick_count = 0
        self.state = IDLE

        # Kennzahlen
        self.completed = 0
        self.met = 0
        self.missed = 0
        self.recent_throughput_min: collections.deque = collections.deque(
            maxlen=THROUGHPUT_WINDOW_JOBS
        )
        self.busy_history: collections.deque = collections.deque(
            maxlen=UTILIZATION_WINDOW_TICKS
        )

        self.nodes: dict[str, object] = {}
        self._start_mqtt()

    # ---- Simulationszeit ----------------------------------------------------
    def _start_mqtt(self):
        def on_message(_client, _userdata, msg):
            try:
                self.sim_now = _parse_sim_time(msg.payload.decode("utf-8"))
                self.tick_count += 1
            except ValueError as exc:
                print(f"[{self.machine_id}] Ungueltiger Tick: {exc}")

        def on_connect(client, _userdata, _flags, _rc, _props=None):
            client.subscribe(TICK_TOPIC)

        self.mqtt = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2, f"{self.machine_id}_worker"
        )
        self.mqtt.on_connect = on_connect
        self.mqtt.on_message = on_message
        self.mqtt.connect_async(MQTT_HOST, MQTT_PORT, 60)
        self.mqtt.loop_start()

    async def _wait_sim_until(self, target: datetime):
        while self.sim_now is None or self.sim_now < target:
            await asyncio.sleep(0.05)

    async def _wait_first_tick(self):
        while self.sim_now is None:
            await asyncio.sleep(0.1)

    # ---- OPC UA Helfer ------------------------------------------------------
    async def _write(self, name: str, value, variant_type: ua.VariantType):
        await self.nodes[name].write_attribute(
            ua.AttributeIds.Value,
            ua.DataValue(ua.Variant(value, variant_type)),
        )

    async def _set_state(self, state: int):
        self.state = state
        await self._write("Zustand", state, ua.VariantType.Int32)

    async def _write_metrics(self):
        recent = self.recent_throughput_min
        mean = sum(recent) / len(recent) if recent else 0.0
        await self._write("AbgeschlosseneJobs", self.completed, ua.VariantType.Int32)
        await self._write("DeadlinesErfuellt", self.met, ua.VariantType.Int32)
        await self._write("DeadlinesVerletzt", self.missed, ua.VariantType.Int32)
        await self._write("DurchlaufzeitMittel", mean, ua.VariantType.Double)

    async def _utilization_loop(self):
        """Pro Tick ein Messpunkt: aktiv (Stufe laeuft) = 1, sonst 0."""
        last_tick = self.tick_count
        while True:
            await asyncio.sleep(0.05)
            if self.tick_count == last_tick:
                continue
            last_tick = self.tick_count
            self.busy_history.append(1 if self.state == ACTIVE else 0)
            pct = 100.0 * sum(self.busy_history) / len(self.busy_history)
            await self._write("Auslastung", pct, ua.VariantType.Double)

    # ---- Energie ------------------------------------------------------------
    async def _acquire_power(self, power_w: float):
        """Wartet, bis die Anschlussleistung fuer die Stufe reicht (Wartelogik)."""
        while True:
            avail = float(await self.nodes["VerfuegbareLeistung"].read_value())
            if power_w <= avail:
                await self._write("Power", power_w, ua.VariantType.Double)
                # Der Server verteilt die Leistung im 0.5-s-Takt neu; danach pruefen,
                # ob eine parallel startende Maschine uns die Leistung weggenommen hat.
                await asyncio.sleep(0.3)
                avail = float(await self.nodes["VerfuegbareLeistung"].read_value())
                if power_w <= avail:
                    return
                await self._write("Power", 0.0, ua.VariantType.Double)
            await self._set_state(WAITING_POWER)
            await asyncio.sleep(0.1 + random.random() * 0.2)

    # ---- Job ----------------------------------------------------------------
    async def _execute_job(self, job_id: str, job_type: int, release, deadline):
        profile = PROFILES[self.machine_type].get(job_type)
        if profile is None:
            raise ValueError(f"Unbekannter Jobtyp {job_type}")

        await self._wait_first_tick()
        start = self.sim_now
        await self._write("startzeit", start, ua.VariantType.DateTime)
        print(f"[{self.machine_id}] Starte '{job_id}' (Typ {job_type}) bei {start:%H:%M}")

        for art_idx, (power_w, minutes) in enumerate(profile):
            await self._write("Art", art_idx, ua.VariantType.Int32)
            await self._write("Duration", float(minutes), ua.VariantType.Double)
            await self._acquire_power(power_w)
            await self._set_state(ACTIVE)
            await self._wait_sim_until(self.sim_now + timedelta(minutes=minutes))
            await self._write("Power", 0.0, ua.VariantType.Double)

        end = self.sim_now
        await self._write("endzeit", end, ua.VariantType.DateTime)

        throughput_min = (end - release).total_seconds() / 60.0
        self.completed += 1
        self.recent_throughput_min.append(throughput_min)
        if end <= deadline:
            self.met += 1
        else:
            self.missed += 1
        await self._write_metrics()
        await self._set_state(IDLE)
        print(
            f"[{self.machine_id}] '{job_id}' fertig, Durchlaufzeit "
            f"{throughput_min:.0f} min, Deadline "
            f"{'eingehalten' if end <= deadline else 'VERPASST'}."
        )

    async def run(self):
        while True:
            util_task = None
            try:
                async with self.client:
                    self.ns = await self.client.get_namespace_index(self.ns_uri)
                    machine = await self.client.nodes.objects.get_child(
                        [f"{self.ns}:{self.machine_id}"]
                    )
                    job = await machine.get_child([f"{self.ns}:aktuellerJob"])
                    stage = await machine.get_child([f"{self.ns}:aktuelleStufe"])

                    async def node(parent, name):
                        return await parent.get_child([f"{self.ns}:{name}"])

                    self.nodes = {
                        "Zustand": await node(machine, "Zustand"),
                        "startzeit": await node(machine, "startzeit"),
                        "endzeit": await node(machine, "endzeit"),
                        "VerfuegbareLeistung": await node(machine, "VerfuegbareLeistung"),
                        "AbgeschlosseneJobs": await node(machine, "AbgeschlosseneJobs"),
                        "DeadlinesErfuellt": await node(machine, "DeadlinesErfuellt"),
                        "DeadlinesVerletzt": await node(machine, "DeadlinesVerletzt"),
                        "DurchlaufzeitMittel": await node(machine, "DurchlaufzeitMittel"),
                        "Auslastung": await node(machine, "Auslastung"),
                        "jobId": await node(job, "jobId"),
                        "JobTyp": await node(job, "JobTyp"),
                        "releaseTime": await node(job, "releaseTime"),
                        "deadline": await node(job, "deadline"),
                        "Art": await node(stage, "Art"),
                        "Power": await node(stage, "Power"),
                        "Duration": await node(stage, "Duration"),
                    }
                    print(
                        f"[{self.machine_id}] Verbunden mit {self.endpoint} "
                        f"(Typ {self.machine_type})"
                    )
                    # Reste einer unterbrochenen Stufe (Neustart mitten im Job) aufraeumen,
                    # sonst blockiert eine veraltete Power die Leistungsverteilung.
                    await self._write("Power", 0.0, ua.VariantType.Double)
                    await self._write("Duration", 0.0, ua.VariantType.Double)
                    await self._write("Art", 0, ua.VariantType.Int32)
                    await self._set_state(IDLE)
                    await self._write_metrics()
                    util_task = asyncio.create_task(self._utilization_loop())

                    while True:
                        zustand = int(await self.nodes["Zustand"].read_value())
                        if zustand == ASSIGNED:
                            job_id = str(await self.nodes["jobId"].read_value())
                            try:
                                await self._execute_job(
                                    job_id,
                                    int(await self.nodes["JobTyp"].read_value()),
                                    _as_utc(await self.nodes["releaseTime"].read_value()),
                                    _as_utc(await self.nodes["deadline"].read_value()),
                                )
                            except (ConnectionError, asyncio.CancelledError):
                                raise
                            except Exception as exc:
                                print(f"[{self.machine_id}] Fehler bei '{job_id}': {exc}")
                                await self._write("Power", 0.0, ua.VariantType.Double)
                                await self._set_state(FAILED)
                                await asyncio.sleep(2)
                                await self._set_state(IDLE)
                        await asyncio.sleep(self.poll_interval)

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(
                    f"[{self.machine_id}] Verbindung zum Server fehlgeschlagen/unterbrochen: "
                    f"{exc}. Neuer Versuch in 3s..."
                )
                await asyncio.sleep(3)
            finally:
                if util_task is not None:
                    util_task.cancel()


async def main():
    await asyncio.gather(
        *(
            FdmWorker(
                endpoint=SERVER_URL,
                ns_uri=NS_URI,
                machine_id=machine_id,
                machine_type=MACHINE_TYPE,
                poll_interval=POLL_INTERVAL,
            ).run()
            for machine_id in MACHINE_IDS
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
