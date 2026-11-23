import asyncio
import os
from datetime import datetime, timezone

from asyncua import Client, ua


SERVER_URL = os.getenv("OPC_SERVER_URL", "opc.tcp://opcua-server:4840/haw/cps")
NS_URI = os.getenv("OPC_NS_URI", "http://haw-hamburg.de/cps")
MACHINE_ID = os.getenv("MACHINE_ID", "FDM_1")
MACHINE_TYPE = int(os.getenv("MACHINE_TYPE", "1"))
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "1.0"))


# Typabhängige Stufenprofile für Typ 1, 2 und 3:
# (art_index, name, power_w, duration_sec)
# art_index: 0 = Preheating, 1 = Printing, 2 = Cooling
STAGE_PROFILES: dict[int, list[tuple[int, str, float, float]]] = {
    1: [
        (0, "Preheating", 400.0, 3.0),
        (1, "Printing", 850.0, 6.0),
        (2, "Cooling", 150.0, 2.0),
    ],
    2: [
        (0, "Preheating", 500.0, 4.0),
        (1, "Printing", 1100.0, 8.0),
        (2, "Cooling", 200.0, 3.0),
    ],
    3: [
        (0, "Preheating", 600.0, 5.0),
        (1, "Printing", 1400.0, 10.0),
        (2, "Cooling", 250.0, 3.0),
    ],
}


class FdmWorker:
    def __init__(
        self,
        endpoint: str,
        ns_uri: str,
        machine_id: str,
        machine_type: int,
        poll_interval: float = 1.0,
    ):
        self.endpoint = endpoint
        self.ns_uri = ns_uri
        self.machine_id = machine_id
        self.machine_type = machine_type if machine_type in STAGE_PROFILES else 1
        self.poll_interval = poll_interval
        self.client = Client(self.endpoint)
        self.ns: int | None = None

    async def _write_var(
        self, parent_node, browse_name: str, value, variant_type: ua.VariantType
    ):
        var_node = await parent_node.get_child([f"{self.ns}:{browse_name}"])
        await var_node.write_attribute(
            ua.AttributeIds.Value,
            ua.DataValue(ua.Variant(value, variant_type)),
        )

    async def _get_stage_node(self, machine_node):
        # Im Server-Code heißt der Unterknoten "aktuelleStufe"
        for name in ("aktuelleStufe", "aktuelleSufe"):
            try:
                return await machine_node.get_child([f"{self.ns}:{name}"])
            except Exception:
                continue
        raise RuntimeError(
            f"Knoten 'aktuelleStufe' an Maschine '{self.machine_id}' nicht gefunden."
        )

    async def _execute_job(self, machine_node, stage_node, job_id: str, job_type: int):
        start_dt = datetime.now(timezone.utc)
        stages = STAGE_PROFILES[self.machine_type]

        print(
            f"[{self.machine_id} | Typ {self.machine_type}] Starte Job '{job_id}' "
            f"(JobTyp={job_type})..."
        )

        # Zustand = 1 -> Job läuft / aktiv
        await self._write_var(machine_node, "Zustand", 1, ua.VariantType.Int32)
        await self._write_var(
            machine_node, "startzeit", start_dt, ua.VariantType.DateTime
        )

        for art_idx, stage_name, power_w, duration_sec in stages:
            await self._write_var(stage_node, "Art", art_idx, ua.VariantType.Int32)
            await self._write_var(stage_node, "Power", power_w, ua.VariantType.Double)
            await self._write_var(
                stage_node, "Duration", duration_sec, ua.VariantType.Double
            )
            print(
                f"[{self.machine_id}] Job '{job_id}' -> Stufe '{stage_name}' "
                f"(Art={art_idx}, Power={power_w}W, Duration={duration_sec}s)"
            )
            await asyncio.sleep(duration_sec)

        end_dt = datetime.now(timezone.utc)
        await self._write_var(machine_node, "endzeit", end_dt, ua.VariantType.DateTime)
        # Zustand = 0 -> Idle / bereit für den nächsten Job
        await self._write_var(machine_node, "Zustand", 0, ua.VariantType.Int32)

        print(f"[{self.machine_id}] Job '{job_id}' abgeschlossen.")

    async def run(self):
        while True:
            try:
                async with self.client:
                    self.ns = await self.client.get_namespace_index(self.ns_uri)
                    objects = self.client.nodes.objects
                    machine_node = await objects.get_child([f"{self.ns}:{self.machine_id}"])
                    stage_node = await self._get_stage_node(machine_node)
                    job_node = await machine_node.get_child([f"{self.ns}:aktuellerJob"])
                    job_id_node = await job_node.get_child([f"{self.ns}:jobId"])
                    job_type_node = await job_node.get_child([f"{self.ns}:JobTyp"])
                    zustand_node = await machine_node.get_child([f"{self.ns}:Zustand"])

                    print(
                        f"[{self.machine_id}] Verbunden mit {self.endpoint} "
                        f"(NodeId={machine_node.nodeid}, Typ={self.machine_type})"
                    )

                    last_handled_job_id = ""

                    while True:
                        zustand = int(await zustand_node.read_value())
                        job_id = str(await job_id_node.read_value() or "")
                        job_type = int(await job_type_node.read_value() or 1)

                        # Job ausführen, wenn eine neue jobId vorliegt oder der Server
                        # einen neuen Job per Zustand == 1 (queued/pending) signalisiert hat.
                        should_run = bool(job_id) and (
                            job_id != last_handled_job_id or zustand == 1
                        )

                        if should_run and zustand != 2:
                            try:
                                await self._execute_job(
                                    machine_node, stage_node, job_id, job_type
                                )
                                last_handled_job_id = job_id
                            except Exception as exc:
                                print(
                                    f"[{self.machine_id}] Fehler bei Job '{job_id}': {exc}"
                                )
                                await self._write_var(
                                    machine_node, "Zustand", 3, ua.VariantType.Int32
                                )

                        await asyncio.sleep(self.poll_interval)

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(
                    f"[{self.machine_id}] Verbindung zum Server fehlgeschlagen/unterbrochen: "
                    f"{exc}. Neuer Versuch in 3s..."
                )
                await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(
        FdmWorker(
            endpoint=SERVER_URL,
            ns_uri=NS_URI,
            machine_id=MACHINE_ID,
            machine_type=MACHINE_TYPE,
            poll_interval=POLL_INTERVAL,
        ).run()
    )