# Spark Consumers Documentation
This directory contains all Apache Spark applications responsible for:

* Streaming ingestion from Kafka
* Structured transformation of nested data
* Batch data loading
* Stream–static joins
* Geospatial proximity analysis
* Persistence to Delta Lake with exactly-once guarantees

These components form the **processing layer** of the seismic data platform.

---

# High-Level Architecture

The Spark layer operates between Kafka and downstream analytics:

```
WebSocket Producers
        ↓
Kafka Topics
        ↓
Spark Structured Streaming
        ↓
Delta Lake Tables
        ↓
Analytical Pipelines & Monitoring
```

All jobs are deployed using `spark-submit` inside Docker and share a standardized Spark configuration enabling Delta Lake, checkpointing, and monitoring.

---

# Shared Spark Configuration

All consumers use a common session setup:

* Delta Lake extensions enabled
* Delta catalog configured
* Checkpointing for fault tolerance
* Prometheus metrics enabled
* Optimized shuffle partitions
* Adaptive query execution (where applicable)


---

# 1. Earthquake Streaming Consumer

## Purpose

Processes real-time global earthquake events from Kafka and persists them to Delta Lake in a clean, structured format.

## Data Flow

```
Kafka (earthquake_raw)
        ↓
Structured JSON parsing
        ↓
Schema enforcement
        ↓
Data validation
        ↓
Deduplication
        ↓
Delta Lake (append mode)
```

## Key Characteristics

* Reads from `earthquake_raw` topic
* Starts from earliest offsets
* Uses strict nested schema
* Flattens JSON into relational structure
* Converts timestamps to Spark `TimestampType`
* Filters invalid coordinates
* Drops duplicates using `unid`
* Writes in append mode
* Uses checkpointing for recovery

## Guarantees

* Exactly-once processing via:

  * Kafka offset tracking
  * Delta transaction log
* Automatic recovery from failures
* No data loss on restart

---

# 2. Wolf Seismic Streaming Consumer

## Purpose

Processes high-frequency seismic station measurements from Japan’s Wolf network.

This stream contains detailed ground motion metrics used for:

* Early warning systems
* Intensity analysis
* Proximity calculations

## Data Flow

```
Kafka (wolf_seismic_stream)
        ↓
Raw JSON extraction
        ↓
Schema parsing (30+ fields)
        ↓
Timestamp conversion
        ↓
Coordinate validation
        ↓
Deduplication
        ↓
Delta Lake (append mode)
```

## Key Characteristics

* Reads from `wolf_seismic_stream`
* Complex schema with:

  * PGA / PGV / PGD
  * East-West, North-South, Up-Down axes
  * Intensity metrics
* Preserves `kafka_timestamp` for audit trails
* Filters invalid coordinates early
* Ensures spatial integrity for downstream joins
* Writes to Delta with checkpointing

## Design Goal

Guarantee that only geographically valid seismic data reaches analytical layers such as the proximity analyzer.

---

# 3. Proximity Analyzer (Streaming)

## Purpose

Continuously detects earthquakes near airports.

This supports operational risk monitoring for aviation safety.

## Architecture Type

Stream–Static Join:

* Streaming input: Earthquakes from Delta
* Static dataset: Airports from Delta

## Processing Logic

```
Read earthquake stream (Delta)
        +
Read airport reference table (Delta)
        ↓
Cross join
        ↓
Haversine distance calculation
        ↓
Filter by threshold (< 20000 km)
        ↓
Write proximity events to Delta
```

## Distance Calculation

* Uses Haversine formula
* Implemented via Spark UDF
* Produces great-circle distance in kilometers

## Output

Writes to `proximity_events` Delta table using:

* Append mode
* Checkpointing
* Exactly-once semantics

## Fault Tolerance

If restarted:

* Resumes from checkpoint
* No duplicate proximity events
* No missed earthquakes

---

# 4. Airport Batch Loader

## Purpose

Loads static airport reference data into Delta Lake.

This enables efficient stream–static joins.

## Processing Flow

```
Read CSV with schema
        ↓
Validate required fields
        ↓
Filter invalid records
        ↓
Add metadata timestamps
        ↓
Write to Delta Lake
```

## Validation Rules

* Latitude not null
* Longitude not null
* At least one identifier (ICAO or IDENT)
* Structured schema enforcement

## Output

* Stored in `delta-lake/tables/airports`
* Supports append or overwrite mode
* Includes:

  * `last_update`
  * `processed_at`
  * `processing_date`

---

# 5. Intensity Analyzer

## Purpose

Computes engineered ground-motion intensity metrics using:

* Earthquake data
* Airport distances

## Computations

* Haversine distance
* Estimated PGV
* Estimated PGD
* Intensity classification:

  * Low
  * Moderate
  * High

## Optimization Features

* Broadcast join for airports
* Adaptive query execution
* Configurable model parameters
* Pure Spark SQL expressions (no heavy UDF usage)

## Output

* Writes to `intensity_analysis` Delta table
* Overwrites with schema evolution enabled

---

# Deployment Model

All Spark jobs are deployed using:

* `spark-submit`
* Docker-based Spark master
* Required packages:

  * `spark-sql-kafka`
  * `delta-spark`

Submission scripts:

* Load environment variables
* Configure master connection
* Set deployment mode (client)
* Include required packages
* Enable Delta extensions
* Verify job via Spark Master REST API

This ensures standardized deployment across all consumers.

---

# Exactly-Once Processing Strategy

The system achieves exactly-once semantics through:

### 1. Kafka Offset Tracking

Spark Structured Streaming manages offsets internally.

### 2. Delta Lake Transaction Log

Delta ensures atomic writes.

### 3. Checkpointing

Each streaming job stores:

* Offset state
* Commit metadata
* Progress information

If a job crashes:

* It resumes from the last checkpoint
* No data duplication
* No data loss

---

# Monitoring

All Spark sessions enable:

* Prometheus metrics
* Executor metrics
* Throughput visibility
* Application-level monitoring

This allows integration with observability tools.

---

# Directory Structure

```
spark-consumers/
├── spark_consumer.py                (Earthquake streaming)
├── spark_wolf_seismic_consumer.py   (Wolf streaming)
├── proximity_analyzer.py            (Stream-static join)
├── intensity_analyzer.py            (Geospatial analytics)
├── airport_batch_to_delta.py        (CSV batch loader)
├── view_table.py                    (Debug utility)
```

---

# Design Principles

* Streaming-first architecture
* Schema enforcement at ingestion
* Early data validation
* Separation of concerns
* Delta Lake as single source of truth
* Fault-tolerant recovery
* Production-style deployment
* Reusable Spark session configuration
* Deterministic joins
* Audit-friendly metadata tracking
