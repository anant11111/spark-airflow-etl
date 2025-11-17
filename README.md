# Spark + Airflow ETL Pipeline

## Overview  
This repository contains a scalable ETL pipeline using :contentReference[oaicite:1]{index=1} and :contentReference[oaicite:2]{index=2}.  
Data flows:

1. Raw user events arrive daily  
2. Spark reads the raw data, transforms it (aggregates `id` → Map<name,value>)  
3. Result is written partitioned by date  
4. Airflow orchestrates the pipeline daily at midnight  

## Repository Layout  

```text
.
├── dags/                   # Airflow DAG definitions
│   └── spark_etl_dag.py
├── spark_job/              # Spark job code
│   └── app.py
├── data/
│   ├── input/              # Raw input Parquet(s)
│   └── output/             # ETL output directory (partitioned by date)
├── tests/                  # Unit tests for Spark job
│   └── test_transform.py
├── airflow/                # Airflow home (metadata DB, logs, config)
├── requirements.txt        # Python dependencies
└── README.md               # This quick start guide
