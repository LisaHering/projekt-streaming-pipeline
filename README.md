# NYC Taxi Streaming Pipeline


## PROJECT DESCRIPTION

This data streaming pipeline is a containerized microservice architecture that uses Apache Kafka and Flink for real-time data ingestion and processing from the public NYC Taxi Trip Records. The project includes a Python script simulating a producer that reads the static data set from a PostgreSQL table.
The pipeline is set up as Infrastructure as Code for seamless deployment and orchestration via Docker Compose and ends in a PostgreSQL sink.

The project is part of my Machine Learning training at the IU Akademy (IU-Modul DLMDWWDE02: Projekt Data Engineering).


## ARCHITECTURE

```text
CSV files (trips, zones)
        │  load scripts
        ▼
PostgreSQL: raw_trips, zones
        │
        ▼
Producer (replays trips in event-time order)
        │
        ├──► Kafka topic: pickup_events ───┐
        └──► Kafka topic: dropoff_events ──┤
                                           ▼
                                Flink: read + validate
                  unreadable /    │               │
                      incomplete  │               │  valid events
                                  ▼               ▼        
                            PostgreSQL:       Flink: Join (by trip_id)
                            rejected_events           │
                                          ┌───────────│─────────────┐
                                          │           │             │
                              implausible │     valid │             │ lifecycle
                                    trips │     trips │             │ signals
                                          ▼           ▼             ▼
                                    PostgreSQL:     Flink:      Flink: Gauge
                                    invalid_trips   Enrich      Aggregation
                                                      │             │
                                                      │             │
                                                Flink: Window       │
                                                Aggregation         │
                                                      │             │
                                                      └──────┬──────┘
                                                             ▼
                                                PostgreSQL: aggregates
```


## PREREQUISITES

- Docker Desktop with Docker Compose
- Python 3.11 with the packages from `requirements.txt`

For the data download:
  - a Google account
  - a free Google Cloud project
  - the Python package `pandas-gbq`

Ports available: 5432 (used by PostgreSQL)
If it is already in use, change the left port number in `docker-compose.yml`, e.g. `"127.0.0.1:5433:5432"`, and set `DB_PORT=5433` in `.env` so the load scripts use the same port.


## GETTING THE DATA

Original Source: New York City Taxi and Limousine Commission (TLC)

Access: Mirrored by Google as BigQuery public dataset `bigquery-public-data.new_york_taxi_trips`
(Google account and Google Cloud project required)

Period: 1-31 January 2022

Columns: `pickup_datetime`, `dropoff_datetime`, `passenger_count`, `trip_distance`, `total_amount`, `pickup_location_id`, `dropoff_location_id`

In order to download the trips data, install `pandas-gbq` and run `download_trips.py`:
```bash
    pip install pandas-gbq
    python src/preprocessing/download_trips.py
```
(on first run a browser window asks for authorization)

Output: Five weekly CSV files (total of 2,463,900 rows) in DATA_DIR (default: `data/`)

The `taxi_zone_lookup.csv` is included in the repository in `data/` (source TLC).


## QUICK START

After you have cloned the Git repository, created your virtual environment and started Docker Desktop:

1. Copy `.env.example` to `.env` and provide path for your data directory, a password for PostgreSQL and your Google Cloud project ID.

2. Download data (see GETTING THE DATA above).

3. In order to start Kafka and PostgresSQL run:
```bash
    docker compose up -d kafka postgres
```

4. Install requirements:
```bash
    pip install -r requirements.txt
```

5. Load data (in this order):
```bash
    python src/preprocessing/load_zones.py 
    python src/preprocessing/load_trips.py
```

6. Start all containers and jobs:
```bash
    docker compose up -d
```

7. Check first results after a few minutes:
```bash
    docker compose exec postgres psql -U taxi_user -d taxi -c "SELECT count(*), max(window_end) FROM aggregates WHERE metric_name = 'trips_last_h';"
```
After a few minutes both values start to grow, for example `43 | 2022-01-01 03:35:00` (hours processed | current time stamp). Run the query again to see the progress.
The run is complete when it shows `8927 | 2022-01-31 23:55:00`.

Runtimes: Producer about 3.5 h, processing about 10 h


## VIEWING THE RESULTS

Open a SQL prompt in the database container:

```bash
docker compose exec postgres psql -U taxi_user -d taxi
```

The pipeline writes to three tables:

| Table | Content |
|---|---|
| `aggregates` | One row per metric, window and dimension |
| `invalid_trips` | Trips that violate a business rule, with `invalid_reason` |
| `rejected_events` | Messages that could not be read or validated, with the raw text |

`aggregates` contains 13 metrics:

| Metrics | Window | Dimension |
|---|---|---|
| `trip_count_by_borough`, `avg_passengers_by_borough`, `avg_distance_by_borough` | 24 h, tumbling | dropoff borough |
| `trip_count_by_time_of_day`, `avg_passengers_by_time_of_day`, `avg_distance_by_time_of_day` | 24 h, tumbling | morning, noon, afternoon, evening, night |
| `trips_per_week`, `revenue_by_week`, `revenue_per_mile_by_week` | 7 days, tumbling, Monday to Sunday | ISO week |
| `trips_last_h`, `revenue_last_h`, `avg_price_last_h` | 1 h, sliding every 5 min | `all` |
| `open_trips` | measured once per minute | `all` |

Example queries:

```sql
-- Trips per borough on 15 January 2022
SELECT dimension, value FROM aggregates
WHERE metric_name = 'trip_count_by_borough'
  AND window_start = '2022-01-15'
ORDER BY value DESC;

-- Why were trips sorted out?
SELECT invalid_reason, count(*) FROM invalid_trips GROUP BY 1;

-- Which messages were rejected?
SELECT topic, reason, raw_payload FROM rejected_events;
```

Result of the full run (2,302,150 trips): the daily windows contain
2,219,877 valid trips and `invalid_trips` contains 2,737 trips. Both numbers
are identical to a reference query on the source table `raw_trips`.


## CONFIGURATION

### Settings in `.env`

| Variable | Used by | Default | Meaning |
|---|---|---|---|
| `DB_PASSWORD` | all services, load scripts | none (required) | Password of the PostgreSQL user |
| `DATA_DIR` | download and load scripts | `data` | Folder that contains the CSV files |
| `GCP_PROJECT_ID` | download script | none (required for the download) | Google Cloud project used to run the BigQuery query |
| `DB_PORT` | load scripts | `5432` | Host port of PostgreSQL, change only if 5432 is in use |

### Further settings

These have defaults in the code. To change one for a container, add it to the
`environment` section of the service in `docker-compose.yml`.

| Variable | Service | Default | Meaning |
|---|---|---|---|
| `SPEED_FACTOR` | producer | `600` | Replay speed: 600 means one hour of trips is sent in six seconds |
| `PICKUP_TOPIC` | producer, Flink job | `pickup_events` | Kafka topic for pickup events |
| `DROPOFF_TOPIC` | producer, Flink job | `dropoff_events` | Kafka topic for dropoff events |
| `KAFKA_BOOTSTRAP` | producer, Flink job | `kafka:9092` | Address of the Kafka broker |
| `PARALLELISM` | Flink job | `1` | Number of parallel instances per operator |
| `CHECKPOINT_INTERVAL_MS` | Flink job | `180000` | Interval between Flink checkpoints, `0` disables checkpoints |

### Business rules

These are constants at the top of `flink_job.py`.

| Constant | Value | Meaning |
|---|---|---|
| `OUT_OF_ORDERNESS_SECONDS` | 30 | Tolerance for events that arrive out of order |
| `MAX_DURATION_SECONDS` | 3 hours | Longer trips are stored as invalid |
| `TIMEOUT_SECONDS` | 48 hours | Waiting time for a missing pickup or dropoff |
| `LAST_HOUR_SLIDE_MINUTES` | 5 | Step of the sliding one-hour window |
| `MAX_PASSENGERS` | 8 | Events with more passengers are rejected |


## RUNNING AGAIN

Never run `docker compose down -v`: it deletes all volumes, including the loaded trips in PostgreSQL, so the data would have to be loaded again.
It is necessary to delete the Kafka topics to rerun (step 2), since the producer refuses to run as long as there are still messages in the topics in order to prevent the ingestion of the same messages a second time, 

1. Stop producer and Flink job:
```bash
   docker compose stop producer processing
```

2. Delete the topics and create them again:
```bash
    docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic pickup_events
    docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic dropoff_events
    docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic pickup_events --partitions 1 --replication-factor 1
   docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic dropoff_events --partitions 1 --replication-factor 1
```
    If creating fails with "already exists", wait a few seconds and repeat.

3. Truncate the result tables:
```bash
    docker compose exec postgres psql -U taxi_user -d taxi -c "TRUNCATE aggregates, invalid_trips, rejected_events;"
```

4. 4. Start producer and Flink job with fresh containers:
```bash
   docker compose up -d --force-recreate --no-deps producer processing
```


## DESIGN DECISIONS

- **Event time instead of processing time.** Windows use the timestamps in
  the events, tracked by watermarks. Results do not depend on how fast the
  data is replayed; three full runs produced identical counts.
- **Stateful join.** Pickup and dropoff events arrive in separate topics and
  are joined by `trip_id` with keyed state and event-time timers. Trips whose
  counterpart is still missing after 48 hours are stored as invalid.
- **Unique trip IDs.** The producer assigns a UUID to every trip, so IDs
  cannot collide after a restart.
- **Incremental aggregation.** Each window keeps four running sums instead of
  all its trips, so memory per window stays constant.
- **Two-stage validation.** Unreadable or incomplete messages go to
  `rejected_events` right after reading; trips that are implausible as a
  whole (duration) go to `invalid_trips` after the join. A single bad message
  cannot stop the job.
- **Idempotent sinks with replay.** After a container restart Flink reads the
  topics from the beginning. All inserts use `ON CONFLICT`, so repeated
  records overwrite or skip existing rows instead of duplicating them.
- **Checkpoints.** Flink saves its state every three minutes and resumes from
  the last checkpoint after an internal failure.
- **Data security and protection.** The password is only stored in `.env`,
  PostgreSQL is reachable from localhost only, Kafka is not exposed, and only
  the seven columns needed are downloaded. The data contains no personal
  information.


## LIMITATIONS

- **Finite data set.** The last windows of the month never close, because no
  later event advances the watermark. A continuous stream would not have this
  effect.
- **Producer cannot resume.** If the producer stops halfway, the topics must
  be deleted and the replay started again (see RUNNING AGAIN).
- **Kafka retention.** Kafka keeps events for seven days (default). A replay
  from the beginning is only complete within that period.
- **Checkpoints are stored inside the container.** They survive internal
  restarts of the job, but not a recreated container.
- **Very long trips.** Trips longer than 48 hours are reported as
  `missing_dropoff` instead of `duration_over_3h` (3 trips in January 2022).
- **Not scaled out.** One Kafka partition, parallelism 1, a single broker.
  Raising `PARALLELISM` alone is not enough; more partitions and an adapted
  watermark strategy would be needed.
- **Late events.** Events arriving more than 30 seconds out of order are not
  counted in windows that are already closed.
- **Throughput.** Each result row is committed separately, and the open-trips
  gauge scans all open trips once per minute. The full month takes about ten
  hours on a laptop.


## PROJECT STRUCTURE

```text
projekt-streaming-pipeline/
├── docker-compose.yml        # defines and connects all services
├── .env.example              # template for passwords and local settings
├── requirements.txt          # Python packages for producer and load scripts
├── README.md
├── data/
│   └── taxi_zone_lookup.csv  # zone lookup table (trip CSVs are not in Git)
├── db/
│   └── init.sql              # creates all tables on first start
├── docs/
│   └── checkpoint_analysis_log.txt
├── src/
│   ├── preprocessing/        # one-off scripts, run on the host
│   │   ├── download_trips.py
│   │   ├── load_zones.py
│   │   └── load_trips.py
│   ├── producer/             # microservice: replays trips as Kafka events
│   │   ├── Dockerfile
│   │   └── producer.py
│   └── processing/           # microservice: Flink streaming job
│       ├── Dockerfile
│       ├── requirements.txt
│       └── flink_job.py
└── tests/                    # unit tests
```
Unlike a single Python package, this project consists of two independent microservices. Each service has its own entry point (`producer.py`, `flink_job.py`) and its own Dockerfile, and `docker-compose.yml` is the entry point of the system as a whole. The services do not import code from each other; they communicate only through Kafka and PostgreSQL. For this reason there is no `main.py`, `setup.py` or `__init__.py` in the project root.


## TESTS

