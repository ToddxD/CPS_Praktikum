import asyncio
import datetime as dt
import json
import os
import random
from datetime import datetime, timezone

from asyncua import Server, ua
from mqtt.mqtt_wrapper import MQTTWrapper


DEFAULT_NS_URI = "http://haw-hamburg.de/cps"
DEFAULT_MQTT_HOST = "mqtt-broker"
DEFAULT_MQTT_PORT = 1883
DEFAULT_MQTT_TOPIC = "cps/jobs/#"
DEFAULT_MACHINES = "FDM_1:1,FDM_2:2,FDM_3:3"


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
        self.mqtt_queue: asyncio.Queue = asyncio.Queue()
        self.mqtt: MQTTWrapper | None = None

    async def _add_enum_property(self, parent, name, values, node_id):
        enum_node = await parent.add_object(
            ua.NodeId(node_id, self.namespace), name
        )
        await enum_node.add_variable(
            ua.NodeId(f"{node_id}.EnumStrings", self.namespace),
            "EnumStrings",
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

    async def _assign_job_to_machine(
        self, machine_node, job_id_value, job_type_value, release, deadline
    ) -> bool:
        ns = self.namespace
        instance_job = await machine_node.get_child([f"{ns}:aktuellerJob"])

        job_id_str = str(getattr(job_id_value, "Value", job_id_value))
        job_type_int = int(getattr(job_type_value, "Value", job_type_value))
        release_dt = _parse_dt(getattr(release, "Value", release))
        deadline_dt = _parse_dt(getattr(deadline, "Value", deadline))

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
        return True

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

            machine = await objects_node.add_object(
                ua.NodeId(name, ns),
                name,
                fdm_type.nodeid,
            )

            zustand_var = await machine.add_variable(
                ua.NodeId(f"{name}.Zustand", ns),
                "Zustand",
                0,
                varianttype=ua.VariantType.Int32,
            )
            await zustand_var.set_writable()

            job_obj = await machine.add_object(
                ua.NodeId(f"{name}.aktuellerJob", ns),
                "aktuellerJob",
                job_type.nodeid,
            )
            job_id_var = await job_obj.add_variable(
                ua.NodeId(f"{name}.aktuellerJob.jobId", ns),
                "jobId",
                "",
                varianttype=ua.VariantType.String,
            )
            await job_id_var.set_writable()

            job_typ_var = await job_obj.add_variable(
                ua.NodeId(f"{name}.aktuellerJob.JobTyp", ns),
                "JobTyp",
                1,
                varianttype=ua.VariantType.Int32,
            )
            await job_typ_var.set_writable()
            await self._set_data_type(job_typ_var, job_typ_enum.nodeid)

            rel_var = await job_obj.add_variable(
                ua.NodeId(f"{name}.aktuellerJob.releaseTime", ns),
                "releaseTime",
                now,
                varianttype=ua.VariantType.DateTime,
            )
            await rel_var.set_writable()

            dl_var = await job_obj.add_variable(
                ua.NodeId(f"{name}.aktuellerJob.deadline", ns),
                "deadline",
                now,
                varianttype=ua.VariantType.DateTime,
            )
            await dl_var.set_writable()

            start_var = await machine.add_variable(
                ua.NodeId(f"{name}.startzeit", ns),
                "startzeit",
                now,
                varianttype=ua.VariantType.DateTime,
            )
            await start_var.set_writable()

            end_var = await machine.add_variable(
                ua.NodeId(f"{name}.endzeit", ns),
                "endzeit",
                now,
                varianttype=ua.VariantType.DateTime,
            )
            await end_var.set_writable()

            stage_obj = await machine.add_object(
                ua.NodeId(f"{name}.aktuelleStufe", ns),
                "aktuelleStufe",
                stage_type.nodeid,
            )
            art_var = await stage_obj.add_variable(
                ua.NodeId(f"{name}.aktuelleStufe.Art", ns),
                "Art",
                0,
                varianttype=ua.VariantType.Int32,
            )
            await art_var.set_writable()
            await self._set_data_type(art_var, stufen_enum.nodeid)

            power_var = await stage_obj.add_variable(
                ua.NodeId(f"{name}.aktuelleStufe.Power", ns),
                "Power",
                0.0,
                varianttype=ua.VariantType.Double,
            )
            await power_var.set_writable()

            dur_var = await stage_obj.add_variable(
                ua.NodeId(f"{name}.aktuelleStufe.Duration", ns),
                "Duration",
                0.0,
                varianttype=ua.VariantType.Double,
            )
            await dur_var.set_writable()

            avail_var = await machine.add_variable(
                ua.NodeId(f"{name}.VerfuegbareLeistung", ns),
                "VerfuegbareLeistung",
                1500.0,
                varianttype=ua.VariantType.Double,
            )
            await avail_var.set_writable()

            self.machines[name] = machine

    async def _create_model(self):
        objects = self.server.nodes.objects
        now = datetime.now(timezone.utc)

        enumeration = await objects.add_object(
            ua.NodeId("Enumeration", self.namespace), "Enumeration"
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
            ua.NodeId("JobType", self.namespace), "JobType"
        )
        job_id = await job_type.add_variable(
            ua.NodeId("JobType.jobId", self.namespace),
            "jobId",
            "",
            varianttype=ua.VariantType.String,
        )
        await self._make_mandatory(job_id)
        job_typ = await job_type.add_variable(
            ua.NodeId("JobType.JobTyp", self.namespace),
            "JobTyp",
            1,
            varianttype=ua.VariantType.Int32,
        )
        await self._make_mandatory(job_typ)
        await self._set_data_type(job_typ, job_types.nodeid)
        release_time = await job_type.add_variable(
            ua.NodeId("JobType.releaseTime", self.namespace),
            "releaseTime",
            now,
            varianttype=ua.VariantType.DateTime,
        )
        await self._make_mandatory(release_time)
        deadline = await job_type.add_variable(
            ua.NodeId("JobType.deadline", self.namespace),
            "deadline",
            now,
            varianttype=ua.VariantType.DateTime,
        )
        await self._make_mandatory(deadline)

        stage_type = await self.server.nodes.base_object_type.add_object_type(
            ua.NodeId("StufeType", self.namespace), "StufeType"
        )
        stage_art = await stage_type.add_variable(
            ua.NodeId("StufeType.Art", self.namespace),
            "Art",
            0,
            varianttype=ua.VariantType.Int32,
        )
        await self._make_mandatory(stage_art)
        await self._set_data_type(stage_art, stufen.nodeid)
        power = await stage_type.add_variable(
            ua.NodeId("StufeType.Power", self.namespace),
            "Power",
            0.0,
            varianttype=ua.VariantType.Double,
        )
        await self._make_mandatory(power)
        duration = await stage_type.add_variable(
            ua.NodeId("StufeType.Duration", self.namespace),
            "Duration",
            0.0,
            varianttype=ua.VariantType.Double,
        )
        await self._make_mandatory(duration)

        fdm_type = await self.server.nodes.base_object_type.add_object_type(
            ua.NodeId("FDMType", self.namespace), "FDMType"
        )
        state = await fdm_type.add_variable(
            ua.NodeId("FDMType.Zustand", self.namespace),
            "Zustand",
            0,
            varianttype=ua.VariantType.Int32,
        )
        await self._make_mandatory(state)
        current_job = await fdm_type.add_object(
            ua.NodeId("FDMType.aktuellerJob", self.namespace),
            "aktuellerJob",
            job_type.nodeid,
        )
        await self._make_mandatory(current_job)
        start_time = await fdm_type.add_variable(
            ua.NodeId("FDMType.startzeit", self.namespace),
            "startzeit",
            now,
            varianttype=ua.VariantType.DateTime,
        )
        await self._make_mandatory(start_time)
        end_time = await fdm_type.add_variable(
            ua.NodeId("FDMType.endzeit", self.namespace),
            "endzeit",
            now,
            varianttype=ua.VariantType.DateTime,
        )
        await self._make_mandatory(end_time)
        current_stage = await fdm_type.add_object(
            ua.NodeId("FDMType.aktuelleStufe", self.namespace),
            "aktuelleStufe",
            stage_type.nodeid,
        )
        await self._make_mandatory(current_stage)
        available_power = await fdm_type.add_variable(
            ua.NodeId("FDMType.VerfuegbareLeistung", self.namespace),
            "VerfuegbareLeistung",
            0.0,
            varianttype=ua.VariantType.Double,
        )
        await self._make_mandatory(available_power)

        nonhierarchical = self.server.get_node(
            ua.ObjectIds.NonHierarchicalReferences
        )

        async def add_job(parent, job_id_value, job_type_value, release, deadline):
            parent_node = self.server.get_node(parent)
            ok = await self._assign_job_to_machine(
                parent_node, job_id_value, job_type_value, release, deadline
            )
            return [ua.Variant(ok, ua.VariantType.Boolean)]

        checks_for = await nonhierarchical.add_reference_type(
            ua.NodeId("checksFor", self.namespace),
            "checksFor",
            symmetric=False,
            inversename="isCheckedBy",
        )
        method = await fdm_type.add_method(
            ua.NodeId("FDMType.JobHinzufuegen", self.namespace),
            "JobHinzufuegen",
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
                job_typ = data.get("job_type", 1)
                release = data.get("release_time", datetime.now(timezone.utc))
                default_deadline = (
                    datetime.now(timezone.utc) + dt.timedelta(hours=2)
                ).isoformat()
                deadline = data.get("deadline", default_deadline)

                await self._assign_job_to_machine(
                    machine_node, job_id, job_typ, release, deadline
                )
                print(
                    f"[MQTT] Job '{job_id}' (Typ={job_typ}) zufällig "
                    f"an '{machine_name}' übergeben."
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

        if hasattr(mqtt_client, "on_message"):
            setattr(mqtt_client, "on_message", on_message)

        await self._maybe_call(mqtt_client, "connect")
        await self._maybe_call(mqtt_client, "start")
        await self._maybe_call(mqtt_client, "loop_start")
        await self._maybe_call(mqtt_client, "subscribe", self.mqtt_topic)

    async def setup(self):
        await self.server.init()
        self.server.set_endpoint("opc.tcp://0.0.0.0:4840/haw/cps")
        self.namespace = await self.server.register_namespace(self.ns_uri)

        objects_node = self.server.nodes.objects
        ticker_node = await objects_node.add_object(
            ua.NodeId("Ticker", self.namespace), "Ticker"
        )
        self.tick_variable = await ticker_node.add_variable(
            ua.NodeId("Tick", self.namespace),
            "Tick",
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

        try:
            while True:
                await asyncio.sleep(1)
        finally:
            consumer_task.cancel()
            await asyncio.gather(consumer_task, return_exceptions=True)
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