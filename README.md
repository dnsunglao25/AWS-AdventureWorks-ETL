# AWS Data Engineering Pipeline: SQL Server to S3 (PySpark)

## 🎯 Project Objective
This project demonstrates a production-grade ETL pipeline. I extracted retail data from an on-premise SQL Server, staged it in an AWS S3 "Bronze" layer, and utilized PySpark to perform complex joins and business logic transformations for a "Gold" analytical layer.

## 🏗️ Architecture
- **Source:** Microsoft SQL Server (AdventureWorksDW)
- **Ingestion:** Python (pyodbc/boto3) automated upload to S3.
- **Processing:** PySpark (Spark 3.5) for high-volume data transformation.
- **Storage:** AWS S3 Medallion Architecture (Landing -> Curated).

## 🚀 Key Features
- **Schema Evolution:** Used PySpark's `inferSchema` to handle data types dynamically.
- **Business Logic:** Calculated real-time Profit and Margin metrics during the join phase.
- **Resilience:** Implemented error handling and environment-aware code to bypass local OS limitations.
