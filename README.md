# Urbanmove-etl-pipeline
This project implements a robust ETL (Extract, Transform, Load) data pipeline for UrbanMove, a hypothetical transport analytics platform. It automates the ingestion of raw trip data from ClickHouse, performs cleaning and weather data enrichment outside the database using Python/Pandas, and loads the final analytics-ready dataset into PostgreSQL.

# Project Highlights
- Architecture: Follows a strict ETL pattern, utilising CSVs as intermediate staging files to ensure the database remains a "clean" storage layer.
- Orchestration: Fully managed by Apache Airflow, using a DAG to chain modular Python scripts.
- Data Quality: Includes automated coordinate scrubbing (GPS validation), logical timestamp verification (dropoff > pickup), and deduplication.
- Enrichment: Merges real-time or historical weather conditions with trip data to provide deeper insights into urban mobility patterns.

# Data Source
- Tripdata [.https://github.demo.trial.altinity.cloud:8443]
- Weather Data [.https://www.kaggle.com/datasets/danbraswell/new-york-city-weather-18692022?utm_source=chatgpt.com]
  
# Tech Stack
Python (Pandas), PostgreSQL, ClickHouse API, Docker, and Apache Airflow.

# Repository Structure
- /dags: Airflow DAG definitions.
- /scripts: Modular Python scripts for each ETL stage (Extract, Clean, Enrich, Load).
- /data: Local staging directory for intermediate CSV processing (excluded from version control).



