# Outboxer Proxima: PostgreSQL → RabbitMQ/Kafka/Redis

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Debezium](https://img.shields.io/badge/Debezium-Embedded-green)](https://debezium.io/)
[![Docker](https://img.shields.io/badge/Docker-ready-blue)](https://www.docker.com/)

---

Outboxer Proxima is a robust, production-ready service for implementing the [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html) in Java.  
It captures changes from a PostgreSQL outbox table using [Debezium Engine](https://debezium.io/) and reliably publishes events to Kafka, RabbitMQ or Redis, enabling event-driven microservices architectures.

>**Note:**
This project is a fork of Georg Schwarz's [outboxer-postgres2rabbitmq](https://github.com/jvalue/outboxer-postgres2rabbitmq).
While the original repository was built using Maven and relied on an older version of Debezium, this fork has been updated to use Gradle and the latest stable release of the [Debezium Engine](https://debezium.io/).
Additionally, support has been extended to include `Redis` and `Kafka` consumers alongside `RabbitMQ`.
Feel free to use and adapt this project under the terms of the license.

---

## Table of Contents

- [Features](#features)
- [Usage](#usage)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Configuration](#configuration)
  - [Running with Docker](#running-with-docker)
  - [Running Locally](#running-locally)
- [Usage Example](#usage-example)
- [Testing](#testing)
- [Resilience & Failure Handling](#resilience--failure-handling)
- [Extending Outboxer](#extending-outboxer)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgements](#acknowledgements)

---

## Features

- **Change Data Capture:** Uses Debezium Embedded to monitor PostgreSQL outbox tables.
- **Event Publishing:** Publishes events to RabbitMQ, Kafka, or Redis.
- **Configurable:** Supports configuration via properties file or environment variables.
- **Resilient:** Handles failures, resumes from last processed offset, and supports at-least-once delivery.
- **Docker-ready:** Easily deployable with Docker and Docker Compose.
- **Extensible:** Add new publishers or event sinks with minimal effort.

---

## Usage

Outboxer runs inside a Docker container. You can build the Docker image from the `Dockerfile` located in the root directory.

A running PostgreSQL and RabbitMQ (or Redis) instance is needed.  
You can use the provided `docker-compose.yml` in the `integration-test` folder for local development.

### Build & Run with Docker Compose

```sh
docker-compose -f [docker-compose.yml](http://_vscodecontentref_/0) up --build
```

As an alternative, you can run the services locally without Docker.  
Ensure that PostgreSQL, RabbitMQ (or Redis), and Java are installed and properly configured.

### Build & Run Locally

```sh
./gradlew clean build  --refresh-dependencies   
java -jar target/outboxer-proxima-*.jar

./gradlew :app:run (for debug)
```

---

## Configuration

Outboxer can be configured via a `outboxer.properties` file or environment variables. The following properties are supported:

**Database Properties**
- `database.hostname` The database host name
- `database.port` The database port
- `database.user` The database username
- `database.password` The database password
- `database.dbname` The database name
- `database.server.name` The unique name that identifies this debezium PostgreSQL connector

**Schema Properties**
- `schema.include.list` An optional, comma-separated list of regular expressions that match names of schemas for which you want to capture changes. [Debezium Documentation](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-property-schema-include-list) 

**Table Properties**
- `table.include.list` An optional, comma-separated list of regular expressions that match fully-qualified table identifiers for tables whose changes you want to capture. [Debezium Documentation:](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-property-table-include-list)

**Offset Properties**
- `offset.storage` The name of the Java class that is responsible for persistence of connector offsets. It must implement <…​>.OffsetBackingStore interface.
- `offset.storage.file.filename` The file where outboxer should store its current offset. The offset is a location in 
  Postgres write-ahead log. Debezium uses this offset to track which records have been processed. After a restart 
  outboxer uses the stored offset as the resume point where to start processing records. If outboxer runs inside a 
  container, this file should be storage outside the container (for example in a docker volume). This ensures that the
  file does survive container restarts. Path to file where offsets are to be stored. Required when offset.storage is set to the <…​>.FileOffsetBackingStore.
- `offset.flush.interval.ms` Interval at which to try committing offsets. The default is 1 minute.

**Publisher Properties**
- `publisher.type` The type of publisher: `rabbitmq`, `kafka`, or `redis`
- `publisher.amqp.url` The url to the AMQP broker
- `publisher.amqp.exchange` The AMQP exchange on which the events should be published.
- `publisher.kafka.bootstrap.servers` The Kafka bootstrap servers (if using Kafka)
- `publisher.amqp.retries` How often a failed event publication should be retried
- `publisher.amqp.retry.delay` The delay before performing retry
- `publisher.redis.host` Redis host (if using Redis)
- `publisher.redis.port` Redis port

**Transformer Properties**
- `transforms.outbox.table.field.event.Id` The name of the column containing the unique event id
- `transforms.outbox.table.field.event.routing_key` The name of the column containing the event routing key
- `transforms.outbox.table.field.event.AggregagteId` The name of the column containing the AggregateName and Id
- `transforms.outbox.table.field.event.EventName` The name of the column containing the EventName
- `transforms.outbox.table.field.event.Payload` The name of the column containing the event payload
- `transforms.outbox.table.field.event.CreateDate` The name of the column containing the event CreateDate

Note: all properties can also be passed via environment variables, but they must be prefixed with `outboxer.` and can be
written in `UPPER_UNDERSCORE` format (e.g. `OUTBOXER_DATABASE_HOSTNAME`).

---

## Architecture

The outboxer service consists of three main components:
- `Publisher (AMQP/Kafka/Redis)`: Publishes events to RabbitMQ, Kafka, or Redis.
- `Outboxer` This is the main component which clues everything together. It is responsible for configuring, starting and
  stopping the DebeziumEngine.
- `AmqpPublisher` This a `DebeziumEngine.ChangeConsumer` that gets called by the DebeziumEngine to handle the change 
  records. From each change record it will create an AMQP message and publishes it to the configured AMQP exchange.
- `OutboxTableTransform` This transformation extracts the unique event id, the routing key, and the payload from the raw
  change records. It does also discard update and delete change records because once an event is published (e.g. added 
  to the outbox table) it should neither be changed nor deleted.

```mermaid
flowchart TD
    subgraph PostgreSQL
        A[Outbox Table]
    end
    subgraph Outboxer
        B[Debezium Embedded Engine]
        C[OutboxTableTransform]
        D[Publisher]
    end
    subgraph Broker
        E[RabbitMQ/Redis/Kafka]
    end
    A -- WAL Changes --> B
    B -- Change Events --> C
    C -- Event Payload --> D
    D -- Publish --> E
```

---
### 🗂️ Project Structure
```
outboxer-proxima-postgres-rabbitmq-redis/
├── app/
│   ├── src/
│   │   ├── main/
│   │   │   └── java/
│   │   │       └── org/
│   │   │           └── jvalue/
│   │   │               └── outboxer/
│   │   │                   ├── Outboxer.java
│   │   │                   ├── AmqpPublisher.java
│   │   │                   ├── RedisPublisher.java
│   │   │                   ├── ConfigHelper.java
│   │   │                   └── OutboxTableTransform.java
│   │   └── test/
│   │       └── java/
│   │           └── org/
│   │               └── jvalue/
│   │                   └── outboxer/
│   │                       └── OutboxerTest.java
├── integration-test/
│   └── docker-compose.yml
├── Dockerfile
├── gradle/
│   └── libs.versions.toml
├── build.gradle
├── settings.gradle
├── README.md
└── outboxer.properties
```

## Getting Started
### Prerequisites
  - Java 17+
  - Gradle 7+
  - Docker (for containerized deployment)
  - Running PostgreSQL instance
  - RabbitMQ or Redis instance

### Compatibility Matrix
To avoid issues conflicts between your Debezium version and the Kafka Connect API, refer to the following matrix
```
Debezium 3.1.1.Final → Kafka Connect 3.0.x - 3.1.x
Debezium 2.7.3.Final → Kafka Connect 3.7.x - 3.8.x
Debezium 2.6.x → Kafka Connect 3.6.x - 3.7.x
```

### Configuration
Outboxer can be configured via an ```outboxer.properties``` file or environment variables.


#### ```outboxer.properties```

```database.hostname=localhost
database.port=5432
database.user=postgres
database.password=postgres
database.dbname=outbox
database.server.name=outboxer-connector

schema.include.list=public
table.include.list=public.outbox

offset.storage=org.apache.kafka.connect.storage.FileOffsetBackingStore
offset.storage.file.filename=/data/outboxer-offsets.dat
offset.flush.interval.ms=60000

publisher.type=rabbitmq
publisher.amqp.url=amqp://guest:guest@rabbitmq:5672/
publisher.amqp.exchange=outbox.events
publisher.amqp.retries=5
publisher.amqp.retry.delay=2000

# publisher.type=redis
# publisher.redis.host=redis
# publisher.redis.port=6379

transforms.outbox.table.field.event.Id=id
transforms.outbox.table.field.event.routing_key=routing_key
transforms.outbox.table.field.event.AggregagteId=aggregate_id
transforms.outbox.table.field.event.EventName=event_name
transforms.outbox.table.field.event.Payload=payload
transforms.outbox.table.field.event.CreateDate=created_at 
```

#### ```Database Properties```
```
database.hostname=localhost
database.port=5432
database.user=postgres
database.password=postgres
database.dbname=outbox
database.server.name=outboxer-connector
```

#### ```# Schema/Table Properties```
```
schema.include.list=public
table.include.list=public.outbox
```

#### ```Offset Storage```
```
offset.storage=org.apache.kafka.connect.storage.FileOffsetBackingStore
offset.storage.file.filename=/data/outboxer-offsets.dat
offset.flush.interval.ms=60000
```

#### ```Publisher Properties (RabbitMQ)```
```
publisher.type=rabbitmq
publisher.amqp.url=amqp://guest:guest@rabbitmq:5672/
publisher.amqp.exchange=outbox.events
publisher.amqp.retries=5
publisher.amqp.retry.delay=2000
```

#### ```Publisher Properties (Redis)```
```
publisher.type=redis
publisher.redis.host=redis
publisher.redis.port=6379
```

#### ```Transformer Properties```
```
transforms.outbox.table.field.event.Id=id
transforms.outbox.table.field.event.routing_key=routing_key
transforms.outbox.table.field.event.AggregagteId=aggregate_id
transforms.outbox.table.field.event.EventName=event_name
transforms.outbox.table.field.event.Payload=payload
transforms.outbox.table.field.event.CreateDate=created_at
```

#### Environment Variables
All properties can also be set as environment variables, prefixed with ```OUTBOXER_``` and in ```UPPER_UNDERSCORE``` format:
```
export OUTBOXER_DATABASE_HOSTNAME=localhost
export OUTBOXER_PUBLISHER_TYPE=rabbitmq
```

### Running with Docker
Build the Docker image:
```
docker build -t outboxer-proxima .
```
Run with Docker Compose (recommended for local development):
```
docker-compose -f docker-compose.yml up --build
```

> **Note:**
> Ensure your offset.storage.file.filename is mapped to a Docker volume for persistence.

### Running Locally
Build the project:
```
./gradlew build
```
Run the application:
```
./gradlew :app:run
```
Or run the JAR directly:
```
java -jar [app.jar](http://_vscodecontentref_/3)
```

## Usage Example
Java API
```
import org.jvalue.outboxer.Outboxer;

public class Main {
    public static void main(String[] args) {
        Outboxer outboxer = new Outboxer();
        outboxer.init();
        outboxer.start();

        // ... application logic ...

        outboxer.stop();
    }
}
```
## Testing
Unit and integration tests are located in ```app/src/test/java/org/jvalue/outboxer/```.

Run all tests:

```./gradlew test```

Example test (JUnit):

```
@Test
public void testOutboxerInitialization() {
    Outboxer outboxer = new Outboxer();
    assertDoesNotThrow(outboxer::init);
}
```

## Resilience & Failure Handling

Outboxer is very resilience to any kind of failure. 

- #### **Offset Storage:**
  Outboxer stores its current offset (Postgres WAL position) in a file. This ensures that after a restart, Outboxer resumes from the last processed event.
  

  > If running in a container, mount the offset file to a Docker volume for persistence.
  
  The offset in the write-ahead log of the last processed event is 
stored in a file (see `offset.storage.file.filename` property). Outboxer does restore the current offset from this file
after a restart and will only process events that have happened after this offset. 

- #### **At least-once Delivery:**

  Because a new offset can not be
written to disk atomically after the event has been processed, outboxer can only provide at least once delivery for the
events. Therefore, idempotent event consumer should be used, so duplicate events do not cause any issues.

- #### **Failure Scenarios:**
  - **PostgreSQL becomes unavailable**: If the connection the database is broken, debezium will determine the error kind. 
  If it is a retriable error, debezium will automatically wait some time (configurable with the 
  `retriable.restart.connector.wait.ms` property) before restarting the database connector. If the restart fails, or a
  non retriable error was thrown initially outboxer will terminate. The operator is then responsible to resolve the
  issue and start outboxer again.
  - **AMQP broker becomes unavailable**: If the publication of an event is failing, retries are performed as configured
  with the `publisher.amqp.retries` and `publisher.amqp.retry.delay` properties. If all retries are failing, outboxer
  will terminate. Then the operator is responsible to resolve the issue and restart outboxer.
  - **Outboxer crashes**: If outboxer is restarted after a crash, it will read the stored offset and will start processing
  events that have happened after the current offset.

## Extending Outboxer
To add a new publisher (e.g., `IBMMq`):

- Implement a new publisher class (e.g., `IBMMqPublisher`).
- Register it in `Outboxer.java` based on the `publisher.type` property.
- Add new configuration options as needed.

## Contributing
Contributions are welcome! Please:

- Open issues for bugs or feature requests.
- Fork and submit pull requests for improvements.
- Follow [Conventional Commits](https://www.conventionalcommits.org/) for commit messages.

## License
This project is licensed under the MIT License.

## Acknowledgements
- [Debezium](https://debezium.io/)
- [RabbitMQ](https://www.rabbitmq.com/)
- [Redis](https://redis.io/)
- [Spring AMQP](https://spring.io/projects/spring-amqp)
- [Microservices.io](https://microservices.io/)
