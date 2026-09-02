# PostgreSQL Kafka CDC Pipeline

A near-real-time PostgreSQL change data capture pipeline built with
Debezium, Apache Kafka, Kafka Connect, Avro, Schema Registry, and
PostgreSQL JDBC Sink.

## Architecture

PostgreSQL WAL → Debezium CDC → Kafka → JDBC Sink → PostgreSQL

## Current Progress

- [x] Three-node Kafka KRaft cluster
- [x] PostgreSQL source and target
- [x] PostgreSQL logical replication
- [x] Debezium PostgreSQL connector
- [x] Initial snapshot
- [x] Avro and Schema Registry
- [x] DELETE tombstone event
- [ ] JDBC Sink with upsert and delete
- [ ] Dead Letter Queue
- [ ] Failure testing
- [ ] Monitoring and latency benchmark
- [ ] Data generator

> This project is currently under active development.