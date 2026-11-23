import asyncio
import collections
import datetime as dt
import json
import os
import random
from datetime import datetime, timezone

from asyncua import Server, ua
from mqtt.mqtt_wrapper import MQTTWrapper


DEFAULT_NS_URI = "http://haw-hamburg.de/cps"
DEFAULT_MQTT_HOST = "mqttbroker"
DEFAULT_MQTT_PORT = 1883
DEFAULT_MQTT_TOPIC = "generator/jobs"
DEFAULT_POWER_LIMIT_W = 2000.0  # feste Anschlussleistung des Produktionssystems
# Instanzname:Maschinentyp -- je Typ 3 Instanzen (Name: FDM_<Typ>_<Nr>)
DEFAULT_MACHINES = ",".join(f"FDM_{t}_{i}:{t}" for t in (1, 2, 3) for i in (1, 2, 3))


def _parse_dt(value) -> datetime:
    if isinstance(value, datetime):
        dt_val = value
    elif isinstance(value, (int, float)):
        dt_val = datetime.fromtimestamp(value, tz=timezone.utc)
    elif isinstance(value, str) and value:
        dt_val = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        dt_val = datetime.now(timezone.utc)
    if dt_val.tzinfo is None:
        dt_val = dt_val.replace(tzinfo=timezone.utc)
    return dt_val


class OpcUaServer:
    def __init__(self):
        self.server = Server()
        self.ns_uri = os.getenv("OPC_NS_URI", DEFAULT_NS_URI)
        self.mqtt_host = os.getenv("MQTT_HOST", DEFAULT_MQTT_HOST)
        self.mqtt_port = int(os.getenv("MQTT_PORT", str(DEFAULT_MQTT_PORT)))
        self.mqtt_topic = os.getenv("MQTT_TOPIC", DEFAULT_MQTT_TOPIC)
        self.machines_spec = os.getenv("OPC_MACHINES", DEFAULT_MACHINES)
        self.namespace = None
        self.machines: dict[str, object] = {}
        self.queues: dict[str, collections.deque] = {}
        self._last_queue_len: dict[str, int] = {}
        self.power_limit = float(os.getenv("POWER_LIMIT_W", str(DEFAULT_POWER_LIMIT_W)))
        self.mqtt_queue: asyncio.Queue = asyncio.Queue()
        self.mqtt: MQTTWrapper | None = None

    async def _add_enum_property(self, parent, name, values, node_id):
        enum_node = await parent.add_object(
            ua.NodeId(node_id, self.namespace), ua.QualifiedName(name, self.namespace)
        )
        await enum_node.add_variable(
            ua.NodeId(f"{node_id}.EnumStrings", self.namespace),
            ua.QualifiedName("EnumStrings", self.namespace),
            [ua.LocalizedText(value) for value in values],
            varianttype=ua.VariantType.LocalizedText,
        )
        return enum_node

    async def _set_data_type(self, variable, data_type):
        await variable.write_attribute(
            ua.AttributeIds.DataType,
            ua.DataValue(ua.Variant(data_type, ua.VariantType.NodeId)),
        )

    async def _make_mandatory(self, node):
        await node.add_reference(
            ua.ObjectIds.ModellingRule_Mandatory,
            ua.ObjectIds.HasModellingRule,
            forward=True,
            bidirectional=False,
        )

    def _enqueue_job(self, machine_name, job_id, job_type, release, deadline) -> bool:
        """Job in die Warteschlange der Maschine legen (Start erledigt _dispatch_loop)."""
        if machine_name not in self.queues:
            return False
        self.queues[machine_name].append(
            (
                str(getattr(job_id, "Value", job_id)),
                int(getattr(job_type, "Value", job_type)),
                _parse_dt(getattr(release, "Value", release)),
                _parse_dt(getattr(deadline, "Value", deadline)),
            )
        )
        return True

    async def _start_job_on_machine(self, machine_node, job) -> None:
        ns = self.namespace
        job_id_str, job_type_int, release_dt, deadline_dt = job
        instance_job = await machine_node.get_child([f"{ns}:aktuellerJob"])

        await (await instance_job.get_child([f"{ns}:jobId"])).write_value(
            job_id_str
        )
        await (await instance_job.get_child([f"{ns}:JobTyp"])).write_attribute(
            ua.AttributeIds.Value,
            ua.DataValue(ua.Variant(job_type_int, ua.VariantType.Int32)),
        )
        await (
            await instance_job.get_child([f"{ns}:releaseTime"])
        ).write_value(release_dt)
        await (await instance_job.get_child([f"{ns}:deadline"])).write_value(
            deadline_dt
        )

        # Zustand = 1 signalisiert dem FDM-Worker, dass ein neuer Job anliegt
        zustand_node = await machine_node.get_child([f"{ns}:Zustand"])
        await zustand_node.write_attribute(
            ua.AttributeIds.Value,
            ua.DataValue(ua.Variant(1, ua.VariantType.Int32)),
        )

    async def _write_int(self, machine_node, name, value) -> None:
        node = await machine_node.get_child([f"{self.namespace}:{name}"])
        await node.write_value(ua.Variant(value, ua.VariantType.Int32))

    async def _dispatch_loop(self):
        """Gibt freien Maschinen (Zustand 0) den naechsten Job aus ihrer Warteschlange
        und verteilt die Anschlussleistung (VerfuegbareLeistung = Limit - Leistung
        aller anderen Maschinen)."""
        ns = self.namespace
        # Knoten einmal aufloesen (Browse-Pfade pro Zyklus waeren bei vielen Maschinen teuer)
        nodes = {}
        for name, node in self.machines.items():
            nodes[name] = {
                "power": await node.get_child([f"{ns}:aktuelleStufe", f"{ns}:Power"]),
                "avail": await node.get_child([f"{ns}:VerfuegbareLeistung"]),
                "state": await node.get_child([f"{ns}:Zustand"]),
            }
        while True:
            try:
                power = {}
                for name in self.machines:
                    power[name] = float(await nodes[name]["power"].read_value() or 0.0)
                total = sum(power.values())

                for name, node in self.machines.items():
                    avail = max(0.0, self.power_limit - (total - power[name]))
                    await nodes[name]["avail"].write_value(
                        ua.Variant(avail, ua.VariantType.Double)
                    )

                    queue = self.queues[name]
                    zustand = int(await nodes[name]["state"].read_value())
                    if zustand == 0 and queue:
                        job = queue.popleft()
                        await self._start_job_on_machine(node, job)
                        print(f"[Dispatch] '{job[0]}' (Typ {job[1]}) startet auf {name}.")
                    if self._last_queue_len.get(name) != len(queue):
                        await self._write_int(node, "Warteschlange", len(queue))
                        self._last_queue_len[name] = len(queue)
            except Exception as exc:
                print(f"[Dispatch] Fehler: {exc}")
            await asyncio.sleep(0.1)

    async def _create_machine_instances(
            self, objects_node, fdm_type, job_type, stage_type
        ):

        ns = self.namespace
        now = datetime.now(timezone.utc)

        enum_node = await objects_node.get_child([f"{ns}:Enumeration"])
        stufen_enum = await enum_node.get_child([f"{ns}:Stufen"])
        job_typ_enum = await enum_node.get_child([f"{ns}:JobTyp"])

        job_type = await self.server.nodes.base_object_type.get_child([f"{ns}:JobType"])
        stage_type = await self.server.nodes.base_object_type.get_child([f"{ns}:StufeType"])
        stufen_enum = await (
            await objects_node.get_child([f"{ns}:Enumeration"])
        ).get_child([f"{ns}:Stufen"])
        job_typ_enum = await (
            await objects_node.get_child([f"{ns}:Enumeration"])
        ).get_child([f"{ns}:JobTyp"])

        for entry in self.machines_spec.split(","):
            entry = entry.strip()
            if not entry:
                continue
            name, _, _ = entry.partition(":")
            name = name.strip()

            # Die Mandatory-Kinder des FDMType (Zustand, aktuellerJob, ...) legt
            # asyncua beim Instanziieren selbst an; hier nur holen und befuellen.
            machine = await objects_node.add_object(
                ua.NodeId(name, ns),
                ua.QualifiedName(name, ns),
                fdm_type.nodeid,
            )

            async def child(parent, *names):
                return await parent.get_child([f"{ns}:{n}" for n in names])

            job_obj = await child(machine, "aktuellerJob")
            stage_obj = await child(machine, "aktuelleStufe")

            initial = [
                (await child(machine, "Zustand"), 0, ua.VariantType.Int32),
                (await child(job_obj, "jobId"), "", ua.VariantType.String),
                (await child(job_obj, "JobTyp"), 1, ua.VariantType.Int32),
                (await child(job_obj, "releaseTime"), now, ua.VariantType.DateTime),
                (await child(job_obj, "deadline"), now, ua.VariantType.DateTime),
                (await child(machine, "startzeit"), now, ua.VariantType.DateTime),
                (await child(machine, "endzeit"), now, ua.VariantType.DateTime),
                (await child(stage_obj, "Art"), 0, ua.VariantType.Int32),
                (await child(stage_obj, "Power"), 0.0, ua.VariantType.Double),
                (await child(stage_obj, "Duration"), 0.0, ua.VariantType.Double),
                (await child(machine, "VerfuegbareLeistung"), self.power_limit, ua.VariantType.Double),
                (await child(machine, "Warteschlange"), 0, ua.VariantType.Int32),
                (await child(machine, "AbgeschlosseneJobs"), 0, ua.VariantType.Int32),
                (await child(machine, "DeadlinesVerletzt"), 0, ua.VariantType.Int32),
                (await child(machine, "DeadlinesErfuellt"), 0, ua.VariantType.Int32),
                (await child(machine, "DurchlaufzeitMittel"), 0.0, ua.VariantType.Double),
                (await child(machine, "Auslastung"), 0.0, ua.VariantType.Double),
            ]
            for node, value, vtype in initial:
                await node.write_value(ua.Variant(value, vtype))
                await node.set_writable()

            self.machines[name] = machine
            self.queues[name] = collections.deque()

    async def _create_model(self):
        objects = self.server.nodes.objects
        now = datetime.now(timezone.utc)

        enumeration = await objects.add_object(
            ua.NodeId("Enumeration", self.namespace), ua.QualifiedName("Enumeration", self.namespace)
        )
        stufen = await self._add_enum_property(
            enumeration,
            "Stufen",
            ["Preheating", "Printing", "Cooling"],
            "Stufen",
        )
        job_types = await self._add_enum_property(
            enumeration, "JobTyp", ["1", "2", "3", "4"], "JobTyp"
        )

        job_type = await self.server.nodes.base_object_type.add_object_type(
            ua.NodeId("JobType", self.namespace), ua.QualifiedName("JobType", self.namespace)
        )
        job_id = await job_type.add_variable(
            ua.NodeId("JobType.jobId", self.namespace),
            ua.QualifiedName("jobId", self.namespace),
            "",
            varianttype=ua.VariantType.String,
        )
        await self._make_mandatory(job_id)
        job_typ = await job_type.add_variable(
            ua.NodeId("JobType.JobTyp", self.namespace),
            ua.QualifiedName("JobTyp", self.namespace),
            1,
            varianttype=ua.VariantType.Int32,
        )
        await self._make_mandatory(job_typ)
        await self._set_data_type(job_typ, job_types.nodeid)
        release_time = await job_type.add_variable(
            ua.NodeId("JobType.releaseTime", self.namespace),
            ua.QualifiedName("releaseTime", self.namespace),
            now,
            varianttype=ua.VariantType.DateTime,
        )
        await self._make_mandatory(release_time)
        deadline = await job_type.add_variable(
            ua.NodeId("JobType.deadline", self.namespace),
            ua.QualifiedName("deadline", self.namespace),
            now,
            varianttype=ua.VariantType.DateTime,
        )
        await self._make_mandatory(deadline)

        stage_type = await self.server.nodes.base_object_type.add_object_type(
            ua.NodeId("StufeType", self.namespace), ua.QualifiedName("StufeType", self.namespace)
        )
        stage_art = await stage_type.add_variable(
            ua.NodeId("StufeType.Art", self.namespace),
            ua.QualifiedName("Art", self.namespace),
            0,
            varianttype=ua.VariantType.Int32,
        )
        await self._make_mandatory(stage_art)
        await self._set_data_type(stage_art, stufen.nodeid)
        power = await stage_type.add_variable(
            ua.NodeId("StufeType.Power", self.namespace),
            ua.QualifiedName("Power", self.namespace),
            0.0,
            varianttype=ua.VariantType.Double,
        )
        await self._make_mandatory(power)
        duration = await stage_type.add_variable(
            ua.NodeId("StufeType.Duration", self.namespace),
            ua.QualifiedName("Duration", self.namespace),
            0.0,
            varianttype=ua.VariantType.Double,
        )
        await self._make_mandatory(duration)

        fdm_type = await self.server.nodes.base_object_type.add_object_type(
            ua.NodeId("FDMType", self.namespace), ua.QualifiedName("FDMType", self.namespace)
        )
        state = await fdm_type.add_variable(
            ua.NodeId("FDMType.Zustand", self.namespace),
            ua.QualifiedName("Zustand", self.namespace),
            0,
            varianttype=ua.VariantType.Int32,
        )
        await self._make_mandatory(state)
        current_job = await fdm_type.add_object(
            ua.NodeId("FDMType.aktuellerJob", self.namespace),
            ua.QualifiedName("aktuellerJob", self.namespace),
            job_type.nodeid,
        )
        await self._make_mandatory(current_job)
        # Kinder (aus JobType instanziiert) ebenfalls als Mandatory markieren,
        # sonst werden sie beim Instanziieren der Maschinen uebersprungen.
        for sub in await current_job.get_children():
            await self._make_mandatory(sub)
        start_time = await fdm_type.add_variable(
            ua.NodeId("FDMType.startzeit", self.namespace),
            ua.QualifiedName("startzeit", self.namespace),
            now,
            varianttype=ua.VariantType.DateTime,
        )
        await self._make_mandatory(start_time)
        end_time = await fdm_type.add_variable(
            ua.NodeId("FDMType.endzeit", self.namespace),
            ua.QualifiedName("endzeit", self.namespace),
            now,
            varianttype=ua.VariantType.DateTime,
        )
        await self._make_mandatory(end_time)
        current_stage = await fdm_type.add_object(
            ua.NodeId("FDMType.aktuelleStufe", self.namespace),
            ua.QualifiedName("aktuelleStufe", self.namespace),
            stage_type.nodeid,
        )
        await self._make_mandatory(current_stage)
        for sub in await current_stage.get_children():
            await self._make_mandatory(sub)
        available_power = await fdm_type.add_variable(
            ua.NodeId("FDMType.VerfuegbareLeistung", self.namespace),
            ua.QualifiedName("VerfuegbareLeistung", self.namespace),
            0.0,
            varianttype=ua.VariantType.Double,
        )
        await self._make_mandatory(available_power)

        # Kennzahlen je Maschine (vom FDM-Worker bzw. Dispatcher geschrieben)
        for kpi_name, kpi_val, kpi_vt in (
            ("Warteschlange", 0, ua.VariantType.Int32),
            ("AbgeschlosseneJobs", 0, ua.VariantType.Int32),
            ("DeadlinesVerletzt", 0, ua.VariantType.Int32),
            ("DeadlinesErfuellt", 0, ua.VariantType.Int32),
            ("DurchlaufzeitMittel", 0.0, ua.VariantType.Double),  # Minuten (Sim-Zeit)
            ("Auslastung", 0.0, ua.VariantType.Double),  # Prozent, gleitend
        ):
            kpi = await fdm_type.add_variable(
                ua.NodeId(f"FDMType.{kpi_name}", self.namespace),
                ua.QualifiedName(kpi_name, self.namespace),
                kpi_val,
                varianttype=kpi_vt,
            )
            await self._make_mandatory(kpi)

        nonhierarchical = self.server.get_node(
            ua.ObjectIds.NonHierarchicalReferences
        )

        async def add_job(parent, job_id_value, job_type_value, release, deadline):
            ok = self._enqueue_job(
                str(parent.Identifier), job_id_value, job_type_value, release, deadline
            )
            return [ua.Variant(ok, ua.VariantType.Boolean)]

        checks_for = await nonhierarchical.add_reference_type(
            ua.NodeId("checksFor", self.namespace),
            ua.QualifiedName("checksFor", self.namespace),
            symmetric=False,
            inversename="isCheckedBy",
        )
        method = await fdm_type.add_method(
            ua.NodeId("FDMType.JobHinzufuegen", self.namespace),
            ua.QualifiedName("JobHinzufuegen", self.namespace),
            add_job,
            [
                ua.VariantType.String,
                ua.VariantType.Int32,
                ua.VariantType.DateTime,
                ua.VariantType.DateTime,
            ],
            [ua.VariantType.Boolean],
        )
        await self._make_mandatory(method)
        await method.add_reference(
            available_power,
            checks_for.nodeid,
            forward=True,
            bidirectional=False,
        )

        for number in (1, 2, 3):
            await self.server.nodes.base_object_type.add_object_type(
                ua.NodeId(f"Machine{number}Type", self.namespace),
                f"Machine{number}Type",
            )

        await self._create_machine_instances(objects, fdm_type, job_type, stage_type)

    def _pick_random_machine(self):
        if not self.machines:
            return None, None
        machine_name = random.choice(list(self.machines.keys()))
        return self.machines[machine_name], machine_name

    def _enqueue_mqtt_payload(self, loop: asyncio.AbstractEventLoop, payload):
        if isinstance(payload, (bytes, bytearray)):
            raw = payload.decode("utf-8")
        else:
            raw = str(payload)
        data = json.loads(raw)
        loop.call_soon_threadsafe(self.mqtt_queue.put_nowait, data)

    async def _consume_mqtt_jobs(self):
        while True:
            data = await self.mqtt_queue.get()
            try:
                machine_node, machine_name = self._pick_random_machine()
                if machine_node is None:
                    print("[MQTT] Keine Maschinen-Instanzen vorhanden.")
                    continue

                job_id = data.get("job_id", "")
                try:
                    job_typ = int(data.get("job_type", 1))
                except (TypeError, ValueError):
                    print(f"[MQTT] Unbekannter Jobtyp {data.get('job_type')!r}, nehme 1.")
                    job_typ = 1
                release = data.get("release_time", datetime.now(timezone.utc))
                default_deadline = (
                    _parse_dt(release) + dt.timedelta(hours=2)
                ).isoformat()
                deadline = data.get("deadline", default_deadline)

                self._enqueue_job(machine_name, job_id, job_typ, release, deadline)
                print(
                    f"[MQTT] Job '{job_id}' (Typ={job_typ}) zufällig "
                    f"an '{machine_name}' eingereiht."
                )
            except Exception as exc:
                print(f"[MQTT] Fehler beim Verarbeiten der Nachricht: {exc}")
            finally:
                self.mqtt_queue.task_done()

    async def _maybe_call(self, obj: object, method_name: str, *args):
        fn = getattr(obj, method_name, None)
        if callable(fn):
            res = fn(*args)
            if asyncio.iscoroutine(res):
                return await res
            return res
        return None

    async def _start_mqtt_wrapper(self, loop: asyncio.AbstractEventLoop):
        def on_message(client, userdata, msg):
            payload = getattr(msg, "payload", msg)
            try:
                self._enqueue_mqtt_payload(loop, payload)
            except Exception as exc:
                print(f"[MQTT] Ungültige Nachricht empfangen: {exc}")

        mqtt_client = MQTTWrapper(
            broker_ip=self.mqtt_host,
            broker_port=self.mqtt_port,
        )
        self.mqtt = mqtt_client

        # Der Client hat die Wrapper-Methode on_message schon gebunden; ein Austausch
        # des Attributs wirkt daher nicht. Callback pro Topic registrieren.
        mqtt_client.subscribe(self.mqtt_topic)
        mqtt_client.subscribe_with_callback(self.mqtt_topic, on_message)
        mqtt_client.loop_start()

    async def setup(self):
        await self.server.init()
        self.server.set_endpoint("opc.tcp://0.0.0.0:4840/haw/cps")
        self.namespace = await self.server.register_namespace(self.ns_uri)

        objects_node = self.server.nodes.objects
        ticker_node = await objects_node.add_object(
            ua.NodeId("Ticker", self.namespace), ua.QualifiedName("Ticker", self.namespace)
        )
        self.tick_variable = await ticker_node.add_variable(
            ua.NodeId("Tick", self.namespace),
            ua.QualifiedName("Tick", self.namespace),
            False,
            varianttype=ua.VariantType.Boolean,
        )
        await self.tick_variable.set_writable()
        await self._create_model()

    async def start(self):
        await self.setup()
        await self.server.start()
        print("Server started at opc.tcp://localhost:4840/haw/cps")

        loop = asyncio.get_running_loop()
        await self._start_mqtt_wrapper(loop)
        consumer_task = asyncio.create_task(self._consume_mqtt_jobs())
        dispatch_task = asyncio.create_task(self._dispatch_loop())

        try:
            while True:
                await asyncio.sleep(1)
        finally:
            consumer_task.cancel()
            dispatch_task.cancel()
            await asyncio.gather(consumer_task, dispatch_task, return_exceptions=True)
            if self.mqtt is not None:
                for stop_name in ("loop_stop", "disconnect", "stop", "close"):
                    if hasattr(self.mqtt, stop_name):
                        fn = getattr(self.mqtt, stop_name)
                        res = fn() if callable(fn) else None
                        if asyncio.iscoroutine(res):
                            await res
            await self.server.stop()
            print("Server stopped")


if __name__ == "__main__":
    asyncio.run(OpcUaServer().start())