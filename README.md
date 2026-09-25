# projekt-streaming-pipeline
Microservice-Architektur für die Stream-Prozessierung von Taxi-Daten mit Kafka, Flink und PostgreSQL (Projekt Modul Data Engineering IU Akademie)


## Projektbeschreibung
Streaming-Pipeline mit Kafka, Flink und PostgreSQL (IU-Projekt DLMDWWDE02).


## Setup

Vor dem Start .env.example zu .env kopieren und Passwort eintragen.

**Einmaliger Schritt beim ersten Start (bzw. nach jedem `docker compose down -v`):**
Kafka läuft im Container als non-root User (`appuser`, uid 1000). Ein frisch von Docker angelegtes Volume gehört aber standardmäßig `root`, wodurch Kafka beim Start mit `AccessDeniedException` abbricht. Deshalb einmalig die Rechte auf dem Volume setzen, bevor der Stack hochgefahren wird:

```bash
docker volume create projekt-streaming-pipeline_kafka_data
docker run --rm -v projekt-streaming-pipeline_kafka_data:/data alpine chown -R 1000:1000 /data
```

Danach normal starten:

```bash
docker compose up -d
```

Der Schritt ist nur nötig, wenn das `kafka_data`-Volume neu angelegt wird (erster Start oder nach `down -v`) – bei einem normalen `down`/`up` bleiben die Berechtigungen erhalten.
