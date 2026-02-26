# Delta Lake Storage Layer

## Overview

Delta Lake is the primary storage layer in this seismic batch and streaming pipeline. It provides ACID-compliant, versioned tables that unify streaming and batch workloads, ensuring reliable, exactly-once processing across earthquake, wolf seismic, airport, and derived proximity data.

All Spark applications in this project write to and read from these Delta tables.

Delta Lake acts as the system of record for the entire data platform.

---

## Why Delta Lake in This Project

### 1. ACID Transactions and Exactly-Once Semantics

Streaming jobs write to Delta tables using `writeStream.format("delta").outputMode("append")` with dedicated checkpoint directories.

Delta’s transaction log (`_delta_log`) ensures:
- Atomic commits
- Consistent state after failures
- No duplicate records on recovery
- Coordination between offsets and commits

This guarantees exactly-once processing across Kafka → Spark → Delta.

---

### 2. Unified Storage for Streaming and Batch

This project combines:

- Streaming ingestion (Kafka-based)
- Batch ingestion (CSV airport data)
- Stream-static joins
- Derived analytical tables

Delta enables all workloads to use the same storage format.

Tables:
- `earthquakes`
- `wolf_seismic`
- `airports`
- `proximity_events`

This allows:
- Stream-stream processing
- Stream-static joins
- Consistent analytics across datasets

---

### 3. Fault Tolerance via Checkpointing

Each streaming job maintains a dedicated checkpoint directory under:


## Checkpoints
Checkpoints store:
- Kafka offsets
- Streaming progress
- Commit markers

If a job fails, it resumes from the last committed state without data loss or duplication.

---

### 4. Time Travel and Versioning

Delta Lake transaction logs enable:

- Querying historical versions of tables
- Auditing changes
- Debugging pipeline behavior
- Reproducing past states

Example capability:
- Load a table by version or timestamp using Delta time travel.

---

### 5. Schema Enforcement and Evolution

Delta enforces schema consistency during writes.

This ensures:
- Structured seismic records
- Validated coordinate fields
- Controlled schema changes when needed

Tables support schema evolution where required.


---

## Data Flow into Delta Lake

### Streaming Sources

1. Earthquake data
   - Consumed from Kafka topic: `earthquake_raw`
   - Written to: `tables/earthquakes`

2. Wolf seismic data
   - Consumed from Kafka topic: `wolf_seismic_stream`
   - Filtered for valid coordinates
   - Written to: `tables/wolf_seismic`

Both use structured streaming with checkpointing for reliability.

---

### Batch Source

3. Airports
   - Loaded from CSV
   - Written to: `tables/airports`
   - Used as static reference data in joins

---

### Derived Table

4. Proximity Events
   - Generated via stream-static join
   - Earthquakes (stream) joined with Airports (batch)
   - Distance calculated using Haversine UDF
   - Filtered by distance threshold
   - Written to: `tables/proximity_events`

This table represents analytical output derived from the core datasets.

---

## Write Patterns

- Streaming writes use Delta append mode.
- Each stream has a dedicated checkpoint directory.
- Spark sessions enable Delta extensions:

  - `spark.sql.extensions`
  - `spark.sql.catalog.spark_catalog`

This ensures proper Delta integration across the pipeline.

---

## Operational Utilities

The project includes tools to validate and inspect Delta tables:

- Table existence checks
- Row count validation
- Recent update verification
- Table inspection utilities

The `delta-lake` directory is mounted in containers for debugging and backup purposes.

---

## Reliability Guarantees

Using Delta Lake in this architecture ensures:

- Exactly-once processing
- Atomic writes
- Recoverable streaming jobs
- Unified batch and streaming storage
- Versioned historical data
- Production-grade consistency

Delta Lake is the foundation of storage and reliability in this pipeline.