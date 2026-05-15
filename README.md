# Databricks End-to-End Flight Data Engineering Project

## Project Overview

This project demonstrates an end-to-end modern data engineering pipeline using Databricks, PySpark and Delta LakeI.

The pipeline follows the Medallion Architecture approach:

```text
Raw Data → Bronze Layer → Silver Layer → Gold Layer
```

The project processes flight booking data, transforms it into analytics-ready datasets

---

# Architecture

## Data Flow

```text
Source Data
    ↓
Bronze Layer (Raw Ingestion)
    ↓
Silver Layer (Data Cleaning & Transformation)
    ↓
Gold Layer (Fact & Dimension Tables)

---

# Technologies Used

| Technology | Purpose                           |
| ---------- | --------------------------------- |
| Databricks | Cloud Data Engineering Platform   |
| PySpark    | Distributed Data Processing       |
| Delta Lake | Reliable Data Storage             |
| Python     | ETL Logic                         |
| GitHub     | Version Control                   |

---

# Project Structure

```text
Databricks_End_To_End_Project/
│
├── bronze/
│   └── bronze_layer.py
│
├── gold/
│   ├── fact_gold.py
│   └── gold_dimensions.py
│
├── pipeline/
│   └── dlt_pipeline.py
│
├── config/
│   └── parameters.py
│
├── screenshots/
│   ├── workflow.png
│   └── dashboard.png
│
├── README.md
└── requirements.txt
```

---

# Medallion Architecture

## Bronze Layer

The Bronze layer ingests raw source data into Databricks using Databricks Auto Loader.

### Responsibilities

* Raw data ingestion
* Schema preservation
* Initial loading
* Incremental ingestion
* Streaming file detection
* Schema evolution handling

### Auto Loader

Databricks Auto Loader is used to automatically detect and ingest new files from the source location.

### Benefits of Auto Loader

* Efficient incremental file processing
* Scalable ingestion
* Automatic schema inference
* Reduced operational complexity
* Optimized cloud file discovery

### Incremental Loading Strategy

The pipeline processes only newly arrived data instead of reprocessing the entire dataset.

This improves:

* Performance
* Scalability
* Pipeline efficiency
* Cost optimization

---

## Silver Layer

The Silver layer performs:

* Data cleaning
* Data validation
* Deduplication
* Transformation
* Standardization

---

## Gold Layer

The Gold layer contains analytics-ready business tables.

### Fact Table

#### `fact_gold_booking`

Stores transactional booking information.

### Dimension Tables

#### `dim_passenger`

Passenger-related information.

#### `dim_flight`

Flight-related information.

#### `dim_airport`

Airport-related information.

This structure follows the Star Schema design for efficient analytics.

---

# Workflow Orchestration

Databricks Workflows are used to automate the pipeline execution.

## Workflow Steps

1. Parameters Configuration
2. Bronze Layer Execution
3. DLT Pipeline Execution
4. Dimension Table Creation
5. Fact Table Creation

---


# Key Skills Demonstrated

* Databricks Auto Loader
* Incremental Data Loading
* Data Engineering
* ETL Pipeline Development
* Data Warehousing
* Star Schema Modeling
* Databricks Workflow Automation
* Delta Lake Architecture


---

# Future Improvements

* Real-time streaming ingestion
* CI/CD pipeline integration
* Azure Data Factory integration
* Incremental refresh optimization
* Data quality monitoring


---

# Screenshots

## Databricks Workflow

<img width="1600" height="820" alt="work_flow" src="https://github.com/user-attachments/assets/0d9f7487-a24d-4b7c-ab6c-5a4b9cabb71f" />



# Conclusion

This project demonstrates a complete modern data engineering workflow from raw ingestion to business intelligence reporting using industry-standard tools and architecture.
