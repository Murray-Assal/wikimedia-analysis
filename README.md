# Wikimedia Recent Changes → Kafka → OpenSearch Pipeline

This project provides a complete real-time data pipeline that ingests live Wikimedia *recent changes* events using the Wikimedia EventStreams API, publishes them to **Kafka**, and indexes them into **OpenSearch** for search and analytics. The entire stack is containerized with **Docker Compose** and includes a UI for Kafka topics.

---

## Features

* Kafka (KRaft mode) single-broker setup
* Kafka UI for browsing topics and messages
* Python-based Wikimedia SSE Producer
* Python-based OpenSearch Consumer
* OpenSearch Dashboards for querying and visualization
* Fully local and reproducible streaming architecture

---

## Architecture Overview

```
Wikimedia RecentChange Stream (SSE)
              |
              v
    wikimedia_producer.py
      (Python Kafka Producer)
              |
              v
            Kafka
              |
              v
    opensearch_consumer.py
      (Python Kafka Consumer)
              |
              v
         OpenSearch Index
```

---

## Services Overview

The `docker-compose.yml` file includes the following services:

| Service                   | Description                                                    |
| ------------------------- | -------------------------------------------------------------- |
| **Kafka**                 | Confluent Kafka broker running in KRaft controller/broker mode |
| **Kafka UI**              | Web-based UI for monitoring Kafka topics and partitions        |
| **OpenSearch**            | Search + analytics engine                                      |
| **OpenSearch Dashboards** | Web UI for querying and visualization                          |

---

## Getting Started

### 1. Clone the repository

```
git clone https://github.com/Murray-Assal/wikimedia-analysis.git
cd wikimedia-analysis
```

### 2. Start all services

```
docker compose up -d
```

### 3. Install Python Dependencies

```
pip install -r requirements.txt
```

### 4. Run the Kafka Producer

```
python wikimedia_producer.py --bootstrap-servers localhost:29092 --topic wikimedia.recentchange
```

Enable debug logging:

```
python wikimedia_producer.py --debug
```

### 5. Run the OpenSearch Consumer

```
python opensearch_consumer.py
```

---

## Accessing the UIs

| Tool                      | URL                                            |
| ------------------------- | ---------------------------------------------- |
| **Kafka UI**              | [http://localhost:8080](http://localhost:8080) |
| **OpenSearch REST API**   | [http://localhost:9200](http://localhost:9200) |
| **OpenSearch Dashboards** | [http://localhost:5601](http://localhost:5601) |

---

## OpenSearch Index

All events are indexed into:

```
wikimedia
```

Document IDs are extracted from:

```
change_event["meta"]["id"]
```

---

## Producer Configuration

| Flag                  | Description             | Default                      |
| --------------------- | ----------------------- | ---------------------------- |
| `--bootstrap-servers` | Kafka bootstrap servers | `localhost:29092`            |
| `--topic`             | Kafka topic name        | `wikimedia.recentchange`     |
| `--stream-url`        | Wikimedia SSE endpoint  | Official RecentChange stream |
| `--debug`             | Enable debug logs       | Off                          |

---

## Project Structure

```
.
├── docker-compose.yml
├── wikimedia_producer.py
├── opensearch_consumer.py
├── requirements.txt
└── LICENSE
```

---

## Troubleshooting

### Kafka not reachable

* Ensure the broker port `29092` is open
* Restart Kafka:

```
docker compose restart kafka
```

### OpenSearch failing

Delete and recreate the index:

```
curl -X DELETE localhost:9200/wikimedia
```

### Producer receives no events

The Wikimedia stream may idle temporarily — events will appear shortly.

---

## License

This project is licensed under the **Apache License 2.0**. See the LICENSE file for details.

---
