import asyncio
import os
import random
from asyncua import Client, ua

DEFAULT_NS_URI = "http://haw-hamburg.de/cps"
DEFAULT_SERVER_URL = "opc.tcp://opcua_server:4840/haw/cps"


class ThermostatSimulator:
    def __init__(self, url, ns_uri, pub_interval_ms=200, sigma=0.5):
        self.url = url
        self.ns_uri = ns_uri
        self.pub_interval_ms = pub_interval_ms
        self.sigma = sigma
        self.client: Client | None = None
        self.subscription = None

    async def connect(self):
        self.client = Client(url=self.url)
        await self.client.connect()
        print("Client connected")

    async def subscribe_to_tick(self):
        assert self.client is not None

        ns_idx = await self.client.get_namespace_index(self.ns_uri)

        tick_node = self.client.get_node(ua.NodeId("Tick", ns_idx))
        setpoint_node = self.client.get_node(ua.NodeId("Setpoint", ns_idx))
        actual_node = self.client.get_node(ua.NodeId("ActualTemp", ns_idx))

        handler = SubHandler(setpoint_node, actual_node, self.sigma)

        self.subscription = await self.client.create_subscription(self.pub_interval_ms, handler)
        await self.subscription.subscribe_data_change(tick_node)
        print(f"Subscribed to Tick at ns={ns_idx};s=Tick (publishing={self.pub_interval_ms} ms)")

    async def start(self):
        await self.connect()
        try:
            await self.subscribe_to_tick()
            while True:
                await asyncio.sleep(1)
        finally:
            if self.subscription:
                await self.subscription.delete()
            if self.client:
                await self.client.disconnect()
            print("Client disconnected")


class SubHandler:
    """
    Reagiert auf Tick-Änderungen.
    Bei Tick=True: liest Setpoint und schreibt ActualTemp = Setpoint + gauss(0, sigma).
    """
    def __init__(self, setpoint_node, actual_node, sigma: float):
        self.setpoint_node = setpoint_node
        self.actual_node = actual_node
        self.sigma = sigma

    def datachange_notification(self, node, val, data):
        if val:
            asyncio.create_task(self._update_temperature())

    async def _update_temperature(self):
        setpoint = await self.setpoint_node.read_value()
        actual = setpoint + random.gauss(0, self.sigma)
        await self.actual_node.write_value(actual)
        print(f"Setpoint={setpoint:.2f}°C  ActualTemp={actual:.2f}°C")


if __name__ == "__main__":
    url = os.getenv("OPC_SERVER_URL", DEFAULT_SERVER_URL)
    ns_uri = os.getenv("OPC_NS_URI", DEFAULT_NS_URI)
    simulator = ThermostatSimulator(url, ns_uri)
    asyncio.run(simulator.start())
