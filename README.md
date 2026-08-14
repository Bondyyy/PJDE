# PJDE — Real-Time CDC Data Pipeline

> End-to-end data engineering pipeline using **Apache Spark, MySQL, Apache Kafka, MongoDB, Python, and Docker Compose**.

PJDE demonstrates a complete data pipeline with two complementary flows:

- **Initial Load:** load the initial JSON dataset into MySQL with Spark batch processing.
- **CDC / Streaming:** capture `INSERT`, `UPDATE`, and `DELETE` changes from MySQL, publish them to Kafka, validate events, route invalid records to a Dead Letter Queue (DLQ), and synchronize valid changes to MongoDB through Spark Structured Streaming.

The project is containerized with Docker Compose so the complete environment can be started locally with a small number of commands.

---

## Architecture

![PJDE Pipeline](./assets/pipeline.png)

### Data flow

```text
INITIAL LOAD

JSON Data
   │
   ▼
Spark Batch
   │
   ▼
MySQL
├── users
└── repositories


CDC / STREAMING

MySQL
   │ INSERT / UPDATE / DELETE
   ▼
MySQL Triggers
   │
   ▼
Log Tables
├── users_log
└── repositories_log
   │
   ▼
Producer
   │
   ▼
Kafka RAW: BondyPJDE_raw
   │
   ▼
Validator
   ├── Valid   ──► BondyPJDE_validated ──► Spark Streaming ──► MongoDB
   └── Invalid ──► BondyPJDE_dlq
```

### How the pipeline works

1. **Initial JSON data** is read by Spark and transformed into `users` and `repositories` datasets.
2. **Spark Batch** writes the initial data into MySQL.
3. **MySQL triggers** capture every `INSERT`, `UPDATE`, and `DELETE` operation and append an event to `users_log` or `repositories_log`.
4. The **Producer** polls new log rows and publishes CDC events to the Kafka RAW topic `BondyPJDE_raw`.
5. The **Kafka Validator** validates message structure, required fields, entity type, operation state, data types, and log sequence.
6. Valid events are routed to `BondyPJDE_validated`; invalid events are routed to `BondyPJDE_dlq` together with the validation error.
7. **Spark Structured Streaming** consumes only validated events and applies CDC operations in `foreachBatch`.
8. **MongoDB** is updated using upsert/update/delete semantics for the `users` and `repositories` collections.

---

## Technology Stack

| Technology | Role in PJDE |
|---|---|
| **Python 3.12** | Main application and ETL language |
| **Apache Spark / PySpark 4.2.0** | Batch initial load and Structured Streaming |
| **Apache Kafka 4.3.1** | Event streaming and topic-based message routing |
| **MySQL** | Source relational database and CDC log storage |
| **MongoDB** | Target document database |
| **Docker** | Application/container packaging |
| **Docker Compose** | Multi-container orchestration |
| **Java 17** | JVM runtime required by Spark |
| **kafka-python** | Kafka producer/consumer integration in Python |
| **mysql-connector-python** | Python-to-MySQL connectivity |
| **PyMongo** | MongoDB operations from Python/Spark micro-batches |
| **python-dotenv** | Environment configuration |
| **Pandas / NumPy** | Supporting Python data libraries |

### Spark connectors

The project also uses Spark packages for Kafka and database integration:

```text
org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0
org.mongodb.spark:mongo-spark-connector_2.13:11.1.0
com.mysql:mysql-connector-j:8.0.33
```

---

## Project Structure

```text
PJDE/
├── README.md
├── compose.yaml
├── Dockerfile
├── requirements.txt
├── .dockerignore
├── .env.compose.example
│
├── assets/
│   └── pipeline.png
│
├── config/
│   ├── database_config.py
│   └── spark_config.py
│
├── databases/
│   ├── mongodb_connect.py
│   ├── mysql_connect.py
│   └── schema_manager.py
│
├── sql/
│   ├── schema.sql
│   └── trigger.sql
│
├── src/
│   ├── main.py
│   ├── ETL/
│   │   ├── kafka_validator.py
│   │   ├── spark_streaming.py
│   │   ├── trigger_kafka.py
│   │   └── consumer.py
│   └── spark/
│       ├── mainSpark.py
│       └── spark_write_data.py
│
└── data/
    └── 2015-03-01-17.json
```

`consumer.py` is a simple consumer/debug utility and is not part of the primary Docker runtime path.

---

## Kafka Topics

PJDE creates three Kafka topics automatically:

| Topic | Purpose |
|---|---|
| `BondyPJDE_raw` | Receives CDC events from the MySQL log producer |
| `BondyPJDE_validated` | Contains events that passed validation |
| `BondyPJDE_dlq` | Dead Letter Queue for invalid events |

The active streaming path is:

```text
BondyPJDE_raw
      │
      ▼
Kafka Validator
   ┌──┴───────────────┐
   │                  │
   ▼                  ▼
VALIDATED             DLQ
   │                  │
   ▼                  └──► retained for inspection / manual reprocessing
Spark Streaming
   │
   ▼
MongoDB
```

> **DLQ behavior:** invalid messages are stored in `BondyPJDE_dlq`. The current implementation does **not** automatically retry messages from the DLQ.

---

## Reliability, Retry, and Recovery

PJDE intentionally uses different recovery mechanisms at different stages.

### Initial-load retry

After Spark writes a batch to MySQL, the project validates the written records. If records are missing and there are no extra/wrong records, only the missing records are written again and the validation is repeated.

```text
Spark write to MySQL
       │
       ▼
Validate result
   ┌───┴──────────────────────┐
   │                          │
Success                  Missing only
   │                          │
   ▼                          ▼
 Done                    Write missing rows
                              │
                              ▼
                         Validate again
                              │
                         max 3 retries
```

### Producer recovery

The producer tracks the last processed MySQL log ID in `kafka_checkpoint`.

```text
MySQL log tables
      │
      ▼
Producer
      │
      ├── send events
      ├── flush Kafka producer
      └── save last_log_id
```

After a restart, the producer resumes from records where:

```sql
log_id > last_log_id
```

This is a **checkpoint/resume mechanism**, not a DLQ retry mechanism.

### Validator recovery

The validator uses Kafka consumer group:

```text
BondyPJDE_validator
```

Auto commit is disabled. The RAW offset is committed only after the outgoing message has been successfully written to either the VALIDATED or DLQ topic.

### Spark Streaming recovery

Spark Structured Streaming stores its streaming checkpoint under:

```text
/app/checkpoints/spark_mongo
```

The checkpoint directory is mounted to a persistent Docker volume, allowing the query to resume after a container restart.

### Container restart policy

Long-running runtime services use Docker Compose restart policies so they can recover from process/container failures:

```text
producer
validator
spark-streaming
mysql
mongodb
kafka
```

---

# Installation

## Prerequisites

The recommended way to run PJDE is through Docker Compose.

Install:

- **Docker Desktop** with Docker Compose support
- **Git**

You do **not** need to install Python, Java, Spark, Kafka, MySQL, or MongoDB directly on the host when using the provided Docker environment.

---

## 1. Clone the repository

```bash
git clone <YOUR_REPOSITORY_URL>
cd PJDE
```

---

## 2. Create the environment file

### Windows PowerShell

```powershell
Copy-Item .env.compose.example .env.compose
```

### Linux / macOS

```bash
cp .env.compose.example .env.compose
```

Then edit `.env.compose`.

Example:

```env
# MYSQL
MYSQL_ROOT_PASSWORD=your_password
MYSQL_HOST=mysql
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=your_password
MYSQL_DB_NAME=github_data

# MONGODB
MONGO_ROOT_USERNAME=admin
MONGO_ROOT_PASSWORD=your_password
MONGO_URI=mongodb://admin:your_password@mongodb:27017/?authSource=admin
MONGO_DB_NAME=github_data

# KAFKA
BootstrapServers=kafka:29092

# SPARK
SPARK_CHECKPOINT_LOCATION=/app/checkpoints/spark_mongo

# INITIAL LOAD
SOURCE_DATA_PATH=/app/data/2015-03-01-17.json
```

> Do not commit `.env.compose` because it contains credentials.

---

## 3. Add the initial JSON dataset

By default, the initial-load service expects:

```text
data/2015-03-01-17.json
```

Place the JSON file inside the `data/` directory:

```text
PJDE/
└── data/
    └── 2015-03-01-17.json
```

If a different file is used, update:

```env
SOURCE_DATA_PATH=/app/data/<your-file>.json
```

---

## 4. Validate Docker Compose configuration

```bash
docker compose --env-file .env.compose config
```

If the configuration is valid, Docker Compose prints the resolved configuration without an error.

---

## 5. Build and start the pipeline

```bash
docker compose --env-file .env.compose up -d --build
```

This starts the runtime environment and automatically performs the required one-shot initialization services.

Main long-running services:

```text
mysql
mongodb
kafka
producer
validator
spark-streaming
```

Initialization services:

```text
kafka-storage-init
kafka-init
db-setup
```

`initial-load` is intentionally placed in the `manual` profile and does not run automatically.

---

## 6. Check service status

```bash
docker compose --env-file .env.compose ps -a
```

Expected behavior:

- `mysql`, `mongodb`, and `kafka` should be healthy/running.
- `producer`, `validator`, and `spark-streaming` should remain running.
- `kafka-storage-init`, `kafka-init`, and `db-setup` should normally finish with exit code `0`.

---

## 7. Run the initial load

After the runtime services are up, load the JSON dataset into MySQL:

```bash
docker compose --env-file .env.compose run --rm initial-load
```

The initial load:

1. Reads the JSON file with Spark.
2. Extracts `users` and `repositories`.
3. Writes the records into MySQL.
4. Validates the Spark write.
5. MySQL triggers create CDC log records.
6. The running producer, validator, and Spark Streaming services propagate the generated events through Kafka to MongoDB.

Because the MySQL tables have primary keys, the initial-load logic checks existing keys before writing new records.

---

# Monitoring the Pipeline

## Follow runtime logs

```bash
docker compose --env-file .env.compose logs -f producer validator spark-streaming
```

Press `Ctrl + C` to stop following the logs. This does **not** stop the containers.

Individual service logs:

```bash
docker compose --env-file .env.compose logs -f producer
```

```bash
docker compose --env-file .env.compose logs -f validator
```

```bash
docker compose --env-file .env.compose logs -f spark-streaming
```

---

## List Kafka topics

```bash
docker compose --env-file .env.compose exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --list
```

Expected topics include:

```text
BondyPJDE_raw
BondyPJDE_validated
BondyPJDE_dlq
```

---

## Check validator consumer-group lag

```bash
docker compose --env-file .env.compose exec kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 \
  --describe \
  --group BondyPJDE_validator
```

A `LAG` of `0` means the validator has caught up with the RAW topic at that moment.

---

## Inspect the DLQ

```bash
docker compose --env-file .env.compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 \
  --topic BondyPJDE_dlq \
  --from-beginning \
  --max-messages 10
```

DLQ records contain both the original data and the validation error.

---

# Verify Databases

## MySQL

The host port is:

```text
localhost:3307
```

Open the MySQL CLI inside the container:

```bash
docker compose --env-file .env.compose exec mysql mysql -uroot -p
```

Then:

```sql
USE github_data;

SHOW TABLES;

SELECT COUNT(*) FROM users;
SELECT COUNT(*) FROM repositories;
SELECT COUNT(*) FROM users_log;
SELECT COUNT(*) FROM repositories_log;

SELECT * FROM kafka_checkpoint;
```

---

## MongoDB

The host port is:

```text
localhost:27017
```

Open `mongosh`:

```bash
docker compose --env-file .env.compose exec mongodb \
  mongosh -u admin -p <your_password> --authenticationDatabase admin
```

Then select the project database:

```javascript
use github_data

show collections

db.users.countDocuments({})
db.repositories.countDocuments({})
```

> Remember to switch to `github_data`; the default `mongosh` database may otherwise be `test`.

---

# Test CDC Manually

Once the pipeline is running, you can verify CDC without rerunning the JSON initial load.

Connect to MySQL and execute an INSERT:

```sql
USE github_data;

INSERT INTO users (
    user_id,
    login,
    gravatar_id,
    url,
    avatar_url
)
VALUES (
    999999999,
    'pjde_test_user',
    NULL,
    'https://api.github.com/users/pjde_test_user',
    'https://avatars.githubusercontent.com/u/999999999'
);
```

The expected flow is:

```text
users
  ↓
after_insert_user_log trigger
  ↓
users_log
  ↓
Producer
  ↓
BondyPJDE_raw
  ↓
Validator
  ↓
BondyPJDE_validated
  ↓
Spark Streaming
  ↓
MongoDB users collection
```

Test UPDATE:

```sql
UPDATE users
SET login = 'pjde_test_user_updated'
WHERE user_id = 999999999;
```

Test DELETE:

```sql
DELETE FROM users
WHERE user_id = 999999999;
```

Spark applies the corresponding CDC event to MongoDB using upsert/update/delete operations.

---

# Docker Networking and Ports

| Component | From host machine | Inside Docker network |
|---|---|---|
| MySQL | `localhost:3307` | `mysql:3306` |
| MongoDB | `localhost:27017` | `mongodb:27017` |
| Kafka | `localhost:9092` | `kafka:29092` |

Application containers use Docker service names rather than `localhost`.

---

# Development Workflow

## After changing Python source code

The source code is copied into the Docker image during build. Therefore, changing a `.py` file on the host does **not** automatically update an already-running container.

Rebuild and recreate services:

```bash
docker compose --env-file .env.compose up -d --build
```

For focused debugging, logs can then be followed for the relevant service.

---

## After changing `requirements.txt` or `Dockerfile`

Rebuild the application image:

```bash
docker compose --env-file .env.compose build --no-cache db-setup
```

Then recreate the runtime services:

```bash
docker compose --env-file .env.compose up -d
```

---

## After changing `.env.compose`

Recreate containers so the new environment variables are injected:

```bash
docker compose --env-file .env.compose up -d --force-recreate
```

---

## After changing SQL schema or triggers

`db-setup` manages schema creation, trigger creation, and MongoDB collection setup.

For trigger changes, rerunning/recreating `db-setup` is usually sufficient because the trigger SQL drops and recreates the triggers.

For destructive or incompatible schema changes to existing persistent tables, use a proper migration. During development, if all data may safely be deleted, the entire environment can be reset as described below.

---

# Stop, Restart, and Reset

## Stop containers while keeping data

```bash
docker compose --env-file .env.compose down
```

Named volumes are preserved.

Start again:

```bash
docker compose --env-file .env.compose up -d
```

---

## Destructive full reset

> **Warning:** the following command deletes the project's persistent MySQL, MongoDB, Kafka, Spark checkpoint, and Ivy volumes.

```bash
docker compose --env-file .env.compose down -v
```

Then rebuild/start the project again:

```bash
docker compose --env-file .env.compose up -d --build
```

and rerun the initial load:

```bash
docker compose --env-file .env.compose run --rm initial-load
```

---

# Docker Services

| Service | Type | Responsibility |
|---|---|---|
| `mysql` | Long-running | Source relational database |
| `mongodb` | Long-running | Target document database |
| `kafka` | Long-running | Kafka broker/controller |
| `kafka-storage-init` | One-shot | Prepare Kafka persistent-volume permissions |
| `kafka-init` | One-shot | Create RAW, VALIDATED, and DLQ topics |
| `db-setup` | One-shot | Create/validate MySQL schema, triggers, Mongo collections |
| `producer` | Long-running | Read MySQL log tables and publish CDC events |
| `validator` | Long-running | Validate RAW events and route them to VALIDATED or DLQ |
| `spark-streaming` | Long-running | Consume validated Kafka events and synchronize MongoDB |
| `initial-load` | Manual | Spark batch load from JSON to MySQL |

---

# Design Notes

### MySQL as the source of truth

The source tables are:

```text
users
repositories
```

Both tables use primary keys so repeated initial-load runs can avoid blindly inserting records that already exist.

### Trigger-based CDC

PJDE implements application-level CDC with MySQL triggers. Every data change generates a corresponding log row with an operation state:

```text
INSERT
UPDATE
DELETE
```

### Event validation

The validator checks fields such as:

```text
entity
log_id
state
log_timestamp
```

and entity-specific user/repository fields. It also warns when a `log_id` gap or duplicate/old message is detected.

### MongoDB synchronization

Spark Streaming applies events by primary key:

- `INSERT` → `replace_one(..., upsert=True)`
- `UPDATE` → `$set` / `$unset` with `upsert=True`
- `DELETE` → `delete_one(...)`

---

# Current Limitations and Possible Improvements

The current project is intentionally compact and educational. Potential future improvements include:

- Automated DLQ reprocessing with a dedicated retry topic/service.
- Retry policies with exponential backoff for transient infrastructure errors.
- Metrics and observability with Prometheus/Grafana.
- Schema Registry and Avro/Protobuf serialization.
- Multi-partition Kafka topics and partitioning by entity/primary key.
- Integration and end-to-end automated tests.
- Production-grade secret management instead of plain environment files.
- Database migration tooling for schema evolution.

---

# Quick Command Reference

```bash
# Validate configuration
docker compose --env-file .env.compose config

# Build + start runtime
docker compose --env-file .env.compose up -d --build

# Show all services
docker compose --env-file .env.compose ps -a

# Run initial load manually
docker compose --env-file .env.compose run --rm initial-load

# Follow ETL logs
docker compose --env-file .env.compose logs -f producer validator spark-streaming

# Stop while preserving volumes
docker compose --env-file .env.compose down

# Rebuild after code changes
docker compose --env-file .env.compose up -d --build

# Full destructive reset
docker compose --env-file .env.compose down -v
```

---

## Summary

PJDE demonstrates a complete local data engineering architecture:

```text
JSON
  ↓
Spark Batch
  ↓
MySQL
  ↓
Trigger-based CDC
  ↓
Kafka RAW
  ↓
Validation
  ├── VALIDATED → Spark Structured Streaming → MongoDB
  └── DLQ
```

The project combines **batch ingestion, change data capture, event streaming, validation, dead-letter handling, checkpoint-based recovery, and real-time synchronization** in a reproducible Docker Compose environment.
