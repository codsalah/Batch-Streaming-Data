# Seismic Data Pipeline: Real-time & Batch Processing Platform

## Overview
A unified data pipeline that ingests real-time seismic events and airport data, processes them with Spark (streaming and batch), and analyzes proximity to provide geospatial insights. Airflow orchestrates the workflows while Kafka handles event-driven communication, ensuring reliable and observable operations.

## Architecture

### High-Level Architecture 
<img width="1036" height="583" alt="image" src="https://github.com/user-attachments/assets/140e2cf5-048f-46ad-bfbf-d9e245be4b42" />

 

### Core Components
- **Data Ingestion**: Real-time WebSocket streams from seismic portals and APIs
- **Stream Processing**: Apache Spark Streaming with Kafka integration
- **Batch Processing**: Spark Batch jobs for historical and reference data
- **Storage**: Delta Lake for ACID-compliant, versioned data storage
- **Orchestration**: Apache Airflow DAGs for workflow management
- **Monitoring**: Prometheus + Grafana for metrics visualization
- **Message Broker**: Apache Kafka with Zookeeper coordination

## Features

### Real-time Capabilities
- **Live Earthquake Monitoring**: Ingest data from SeismicPortal.eu WebSocket API
- **Seismic Waveform Streaming**: Process real-time seismic data from WolfX API
- **Continuous Processing**: Spark Streaming jobs for immediate data transformation
- **Event-Driven Architecture**: Kafka-based decoupling of producers and consumers

### Batch Processing
- **Airport Data Integration**: Batch processing of airport reference data
- **Delta Lake Storage**: Reliable, versioned data storage with time travel
- **Scheduled Processing**: Airflow-managed batch workflows

### Analytics & Insights
- **Proximity Analysis**: Real-time earthquake proximity to airports
- **Geospatial Processing**: Haversine distance calculations for location-based insights
- **Multi-source Correlation**: Cross-referencing seismic events with infrastructure data

### Reliability & Monitoring
- **Self-Healing Pipeline**: Automatic restart of failed Spark applications
- **Infrastructure Validation**: Health checks for all pipeline components
- **Comprehensive Metrics**: Prometheus monitoring for producers, Kafka, and Spark
- **Visual Dashboards**: Grafana for real-time pipeline observability

## 🛠️ Technologies Used

| Category              | Technologies                                              
|-----------------------|-----------------------------------------------------------
| **Stream Processing** | Apache Spark 3.5.0, Spark Streaming, Structured Streaming 
| **Message Queue**     | Apache Kafka 7.5.0, Zookeeper                             
| **Batch Processing**  | Apache Spark Batch, Delta Lake 3.0.0                      
| **Orchestration**     | Apache Airflow 2.7.3                                      
| **Storage**           | Delta Lake, PostgreSQL (Airflow metadata)                 
| **Monitoring**        | Prometheus, Grafana, JMX Exporter                         
| **Containerization**  | Docker, Docker Compose                                    
| **Programming**       | Python 3.10, PySpark, Bash                                
| **Data Sources**      | SeismicPortal.eu API, WolfX Seismic API       


## 🚀 Getting Started

### Prerequisites
- Docker 20.10+
- Docker Compose 2.20+

### Installation & Configuration

1. **Clone the Project**
   ```bash
   git clone <repository-url>
   cd Batch-Streaming-Data
   ```

2. **Setup Environment Variables (Mandatory)**
   The project is designed to be fully portable. All configurations are managed via the `.env` file.
   ```bash
   cp .env.example .env
   ```
   Open `.env` and adjust settings for your system (ports, memory, container names, etc.). The defaults in `.env.example` are suitable for most local setups.

   > [!IMPORTANT]
   > Do NOT commit your `.env` file to Git. It is already included in `.gitignore`.

3. **Start the Infrastructure**
   ```bash
   docker-compose up -d
   ```

4. **Access the Services**
   All services will use the ports defined in your `.env` file. By default:
   - **Airflow UI**: http://localhost:8087 (admin/admin)
   - **Spark Master UI**: http://localhost:8082
   - **Kafka UI**: http://localhost:8081
   - **Grafana**: http://localhost:3000 (admin/admin)
 


## 🔧 Running Locally

### Complete Pipeline Startup
```bash
# Start all services
docker-compose up -d

# Wait for services to initialize (2-3 minutes)
sleep 180

# Trigger the full pipeline
docker exec airflow-webserver bash /opt/airflow/scripts/trigger_pipeline_dag.sh

# Monitor progress in Airflow UI: http://localhost:8087
```

 
### Monitoring
1. **Grafana Setup**:
   - Login at http://localhost:3000
   - Add Prometheus data source: `http://prometheus:9090`
   - Import dashboard templates from `/grafana/dashboards/`

2. **Metrics Endpoints**:
   - Earthquake Producer: http://localhost:8000/metrics
   - Wolf Producer: http://localhost:8001/metrics
   - Kafka JMX: http://localhost:9404/metrics

## 📁 Project Structure
```
seismic-data-pipeline/
├── dags/                    # Airflow DAG definitions
│   ├── airport_batch_dag.py
│   ├── seismic_infra_validation_dag.py
│   └── seismic_pipeline_lifecycle_dag.py
├── scripts/                 # Bash scripts for orchestration
│   ├── check_system_resources.sh
│   ├── restart_spark_jobs.sh
│   ├── run_airport_batch.sh
│   └── ...
├── Kafka/                   # Kafka producers
│   ├── producer-earthquakes.py
│   └── producer-wolf_seismic.py
├── spark-consumers/         # Spark streaming jobs
│   ├── airport_batch_to_delta.py
│   ├── proximity_analyzer.py
│   └── ...
├── data/                    # Data files
│   └── airports.csv
├── delta-lake/              # Delta tables storage
├── prometheus/              # Monitoring configuration
├── docker-compose.yml       # Service definitions
└── .env                     # Environment variables
```

## Data Flow

1. **Ingestion Phase**
   - Earthquake data from SeismicPortal.eu WebSocket
   - Seismic waveform data from WolfX API
   - Airport reference data from CSV batch file

2. **Processing Phase**
   - Real-time stream processing via Spark Structured Streaming
   - Batch processing of static datasets
   - Data cleaning, validation, and enrichment

3. **Storage Phase**
   - Stream results to Delta Lake tables
   - Maintain data versioning and time travel
   - Ensure ACID compliance

4. **Analytics Phase**
   - Proximity analysis between earthquakes and airports
   - Real-time alerting and monitoring

5. **Monitoring Phase**
   - Metrics collection via Prometheus
   - Visualization in Grafana dashboards
   - Pipeline health monitoring

 

**Note**: This pipeline processes real seismic data. Monitor resource usage and implement appropriate alerting for production deployments. The system is designed for educational and research purposes regarding data engineering patterns with real-world data sources.
