# CPS Startprojekt – OPC UA, MQTT & FUXA

Dieses Projekt ist der Einstiegspunkt ins Labor. Es zeigt, wie Agenten in einem Cyber-Physical System über **OPC UA** und **MQTT** kommunizieren und wie Daten in **FUXA** visualisiert werden.

Alle Komponenten laufen als Docker-Container und werden über `docker-compose` gestartet.

---

## Architekturübersicht

```
┌─────────────────────────────────────────────────────────┐
│                      OPC UA Pfad                        │
│                                                         │
│  tick_gen  ──write Tick──►  opcua_server  ◄──read──  chaos_sensor
│                                  │                      │
│                             FUXA liest                write ActualTemp
│                           Setpoint & ActualTemp         │
│                                  │                      │
│                           FUXA setzt Setpoint           │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│                      MQTT Pfad                          │
│                                                         │
│  tick_gen_mqtt  ──tickgen/tick──►  mqttbroker  ──►  chaos_sensor_mqtt
│                                                         │
│                                              publishes chaossensor/1/data
└─────────────────────────────────────────────────────────┘
```

---

## Komponenten

### OPC UA Pfad

| Service | Rolle | Beschreibung |
|---|---|---|
| `opcua_server` | Datenhub | Zentraler OPC UA Server, stellt das Information Model bereit |
| `tick_gen` | Taktgeber | Schreibt `Tick` (Boolean) abwechselnd True/False im 1-Sekunden-Rhythmus |
| `chaos_sensor` | Sensor-Simulator | Abonniert `Tick`; bei `Tick=True` liest er den Sollwert und schreibt `ActualTemp = Setpoint + Rauschen` |
| `fuxa` | Visualisierung | HMI-Dashboard; liest OPC UA Werte und erlaubt das Setzen des Sollwerts |

### MQTT Pfad

| Service | Rolle | Beschreibung |
|---|---|---|
| `mqttbroker` | Message Broker | Eclipse Mosquitto |
| `tick_gen_mqtt` | Taktgeber | Veröffentlicht Timestamps auf `tickgen/tick` (simulierte Zeit, einstellbarer Speed Factor) |
| `chaos_sensor_mqtt` | Sensor-Simulator | Empfängt Ticks, generiert Zufallswerte (0–100) und veröffentlicht sie auf `chaossensor/1/data` |

---

## OPC UA Information Model

```
Objects/
├─ Ticker/
│  └─ Tick          (Boolean)   – Takt-Signal; von tick_gen beschrieben
└─ Thermostat/
   ├─ Setpoint      (Double)    – Solltemperatur in °C; von FUXA setzbar
   └─ ActualTemp    (Double)    – Isttemperatur in °C; von chaos_sensor berechnet
```

**Namespace URI:** `http://haw-hamburg.de/cps`
**Namespace Index:** `ns=2`
**NodeIds:** `ns=2;s=Tick` · `ns=2;s=Setpoint` · `ns=2;s=ActualTemp`
**Endpoint:** `opc.tcp://localhost:4840/haw/cps`
**Security:** None (Demo)

---

## MQTT Topics

| Topic | Richtung | Inhalt |
|---|---|---|
| `tickgen/tick` | `tick_gen_mqtt` → alle | ISO-Timestamp der simulierten Zeit |
| `tickgen/speed_factor` | beliebig → `tick_gen_mqtt` | Simulationsgeschwindigkeit (z.B. `10` = 10× Echtzeit) |
| `chaossensor/1/data` | `chaos_sensor_mqtt` → alle | JSON: `{"payload": <int>, "timestamp": <iso>}` |

---

## Quick Start

```bash
# Alle Services starten
docker-compose up --build -d

# Logs beobachten
docker-compose logs -f

# Einzelnen Service beobachten
docker-compose logs -f chaos_sensor

# Alles stoppen
docker-compose down
```

**FUXA Dashboard:** http://localhost:1881
**OPC UA Endpoint:** `opc.tcp://localhost:4840/haw/cps`
**MQTT Broker:** `localhost:8883`

---

## Verzeichnisstruktur

```
src/
├── opcua_server/       # OPC UA Server
├── tick_gen/           # Taktgeber (OPC UA)
├── chaos_sensor/       # Thermostat-Simulator (OPC UA)
├── tick_gen_mqtt/      # Taktgeber (MQTT)
└── chaos_sensor_mqtt/  # Sensor-Simulator (MQTT)
appdata/                # FUXA Konfiguration & Datenbank (persistent)
db/                     # FUXA Zeitreihendaten (persistent)
logs/                   # FUXA Logs
```

---

## Thermostat-Logik

Der `chaos_sensor` simuliert einen realen Temperatursensor mit Messrauschen:

```
ActualTemp = Setpoint + gauss(μ=0, σ=0.5)
```

Der Sollwert (`Setpoint`) kann über FUXA zur Laufzeit geändert werden. Der Sensor reagiert auf jede steigende Flanke des Takt-Signals.

---

**Referenzen:**
[OPC UA](https://opcfoundation.org/) · [asyncua](https://github.com/FreeOpcUa/opcua-asyncio) · [paho-mqtt](https://eclipse.dev/paho/) · [FUXA](https://github.com/frangoteam/FUXA) · [Docker](https://www.docker.com/)
