import asyncio
import os
from asyncua import Server, ua

DEFAULT_NS_URI = "http://haw-hamburg.de/cps"


class OpcUaServer:
    def __init__(self):
        self.server = Server()
        self.ns_uri = os.getenv("OPC_NS_URI", DEFAULT_NS_URI)

    async def setup(self):
        await self.server.init()
        self.server.set_endpoint("opc.tcp://0.0.0.0:4840/haw/cps")
        self.namespace = await self.server.register_namespace(self.ns_uri)

        objects_node = self.server.nodes.objects

        # Taktgeber – wird von tick_gen geschrieben, alle Sensoren abonnieren ihn
        ticker_node = await objects_node.add_object(self.namespace, "Ticker")
        self.tick_variable = await ticker_node.add_variable(
            ua.NodeId("Tick", self.namespace),
            "Tick",
            False,
            varianttype=ua.VariantType.Boolean,
        )
        await self.tick_variable.set_writable()

        # Thermostat
        thermostat_node = await objects_node.add_object(self.namespace, "Thermostat")

        # Sollwert – wird von FUXA gesetzt
        self.setpoint = await thermostat_node.add_variable(
            ua.NodeId("Setpoint", self.namespace),
            "Setpoint",
            20.0,
            varianttype=ua.VariantType.Double,
        )
        await self.setpoint.set_writable()
        await self.setpoint.write_attribute(
            ua.AttributeIds.DisplayName,
            ua.DataValue(ua.LocalizedText("Solltemperatur")),
        )

        # Istwert – wird vom Thermostat-Simulator geschrieben, von FUXA gelesen
        self.actual_temp = await thermostat_node.add_variable(
            ua.NodeId("ActualTemp", self.namespace),
            "ActualTemp",
            20.0,
            varianttype=ua.VariantType.Double,
        )
        await self.actual_temp.set_writable()
        await self.actual_temp.write_attribute(
            ua.AttributeIds.DisplayName,
            ua.DataValue(ua.LocalizedText("Isttemperatur")),
        )

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
    server = OpcUaServer()
    asyncio.run(server.start())

