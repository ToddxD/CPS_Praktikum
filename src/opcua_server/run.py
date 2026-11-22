import asyncio
import os
from datetime import datetime, timezone

from asyncua import Server, ua


DEFAULT_NS_URI = "http://haw-hamburg.de/cps"


class OpcUaServer:
    def __init__(self):
        self.server = Server()
        self.ns_uri = os.getenv("OPC_NS_URI", DEFAULT_NS_URI)

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

    async def _create_model(self):
        objects = self.server.nodes.objects
        now = datetime.now(timezone.utc)

        # The diagram's Enumeration container is represented explicitly so
        # clients can discover the values without relying on implementation
        # specific Python enums.
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

        # Object types mirror the reusable types in the information model.
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
            instance_job = await parent_node.get_child(
                ["0:aktuellerJob"]
            )
            job_id_value = getattr(job_id_value, "Value", job_id_value)
            job_type_value = int(
                getattr(job_type_value, "Value", job_type_value)
            )
            release = getattr(release, "Value", release)
            deadline = getattr(deadline, "Value", deadline)
            await (await instance_job.get_child(["0:jobId"])).write_value(
                job_id_value
            )
            await (await instance_job.get_child(["0:JobTyp"])).write_attribute(
                ua.AttributeIds.Value,
                ua.DataValue(
                    ua.Variant(job_type_value, ua.VariantType.Int32)
                ),
            )
            await (await instance_job.get_child(["0:releaseTime"])).write_value(
                release
            )
            await (await instance_job.get_child(["0:deadline"])).write_value(
                deadline
            )
            return [ua.Variant(True, ua.VariantType.Boolean)]

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

        try:
            while True:
                await asyncio.sleep(1)
        finally:
            await self.server.stop()
            print("Server stopped")


if __name__ == "__main__":
    asyncio.run(OpcUaServer().start())
