import asyncio
import os
from asyncua import Client, ua

DEFAULT_NS_URI = "http://haw-hamburg.de/cps"
DEFAULT_SERVER_URL = "opc.tcp://opcua_server:4840/haw/cps"


class OpcUaClient:
    def __init__(self, url, ns_uri):
        self.url = url
        self.ns_uri = ns_uri
        self.client: Client | None = None

    async def connect(self):
        self.client = Client(url=self.url)
        await self.client.connect()
        print("Client connected")

    async def send_tick(self):
        assert self.client is not None

        # Namespace-Index zur URI auflösen (robust, falls nicht immer ns=2)
        ns_idx = await self.client.get_namespace_index(self.ns_uri)

        # Variable direkt per String-NodeId adressieren (ns=…;s=Tick)
        tick_nodeid = ua.NodeId("Tick", ns_idx)
        tick_variable = self.client.get_node(tick_nodeid)

        print(f"Using NodeId: ns={ns_idx};s=Tick -> {tick_variable}")

        while True:
            print("writing True to tick_variable")
            await tick_variable.write_value(ua.DataValue(ua.Variant(True, ua.VariantType.Boolean)))
            await asyncio.sleep(0.5)
            print("writing False to tick_variable")
            await tick_variable.write_value(ua.DataValue(ua.Variant(False, ua.VariantType.Boolean)))
            await asyncio.sleep(0.5)

    async def start(self):
        await self.connect()
        try:
            await self.send_tick()
        finally:
            if self.client:
                await self.client.disconnect()
            print("Client disconnected")


if __name__ == "__main__":
    url = os.getenv("OPC_SERVER_URL", DEFAULT_SERVER_URL)
    ns_uri = os.getenv("OPC_NS_URI", DEFAULT_NS_URI)
    opcua_client = OpcUaClient(url, ns_uri)
    asyncio.run(opcua_client.start())