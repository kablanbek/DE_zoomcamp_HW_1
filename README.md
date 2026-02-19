# DE_zoomcamp_HW_1

Pipeline designed to extract Green Taxi Data and Taxi Zone Data using Python and further loading to PostgreSQL server. Python script is containerized using custom image described in Dockerfile.

# Pipeline Folder

This folder contains the full local data engineering pipeline setup for the **Data Engineering Zoomcamp**(https://github.com/DataTalksClub/data-engineering-zoomcamp) project.

The pipeline is designed to ingest **NYC Taxi trip data**(https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), load it into PostgreSQL, and orchestrate processing steps using Python scripts and Docker containers.

It includes:

- Dockerized PostgreSQL environment
- Data ingestion script
- SQL analysis
- Supporting configuration files

---

## Folder Contents

### Core Pipeline Files

#### [`ingest_data.py`](pipeline/ingest_data.py)
Standalone ingestion script.

Responsible for:

- reading raw NYC Taxi data (CSV/Parquet)
- cleaning or formatting the dataset
- loading the data into PostgreSQL tables

This script is the key ETL ingestion component.

---

### Docker and Environment Setup

#### [`Dockerfile`](pipeline/Dockerfile)
Defines the custom Docker image used for running the pipeline code.

Includes:

- Python environment setup
- dependency installation
- execution configuration for pipeline scripts

---

#### [`docker-compose.yaml`](pipeline/docker-compose.yaml)
Docker Compose configuration for running the full local stack.

Spins up services such as:

- PostgreSQL database container
- Pipeline execution container

Allows the entire pipeline to run locally with several commands:

```bash
docker compose up

docker run -it \
  --network=pipeline_default \
  taxi_ingest:v001 \
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=ny_taxi \
