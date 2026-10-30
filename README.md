# CPS Praktikum

## Setup
Container Tools für VS Code installieren

## Container starten

Der Mosquitto-Broker, der Job-Generator und FUXA können mit Docker Compose
gestartet werden:

```shell
docker compose up -d
```

MQTT ist anschließend auf Port `1883` und MQTT über WebSockets auf Port `9001`
erreichbar. Der Job-Generator veröffentlicht die generierten Jobs auf dem Topic
`jobs`. Die Ausgaben des Generators können mit folgendem Befehl angezeigt
werden:

```shell
docker compose logs -f job-generator
```

FUXA ist anschließend unter [http://localhost:1881](http://localhost:1881)
erreichbar. Die FUXA-Projekte, Datenbank, Logs und Bilder werden in Docker
Volumes gespeichert.

Die Dateien des Job-Generator-Services liegen im Ordner
`job-generator/`.

Zum Stoppen der Container:

```shell
docker compose down
```
