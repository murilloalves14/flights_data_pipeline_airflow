Data Engineering Project: Automated Data Pipeline with Python, Airflow, and AWS

OVERVIEW

This project showcases an automated data pipeline built using Python, Airflow,
and various AWS services such as EC2, S3, and RDS (PostgreSQL). The data
pipeline is designed to extract, transform, and load data related to airports,
airlines, and flights in the sky of Sao Paulo. The main objective of this project
is to demonstrate proficiency in data engineering and Airflow.

PROJECT STRUCTURE
The project is organized into the following main components:

1. ETL Process: The data pipeline follows the standard Extract, Transform, and Load (ETL) process.

2. Data Sources:

S3 Buckets: Contains data about airports and airlines worldwide.
API: Provides real-time flight data for flights in the sky of Sao Paulo.

3. Technologies Used:

Python: Used for writing scripts and data processing.
Airflow: Manages the workflow and scheduling of the data pipeline.
AWS EC2: Hosts the Airflow platform.
AWS S3: Stores the data extracted from S3 buckets.
AWS RDS (PostgreSQL): Serves as the data warehouse for the processed data.


AIRFLOW DAGs

The data pipeline is defined as an Airflow Directed Acyclic Graph (DAG) 
and can be found in the dags/ directory. The DAG consists of the following tasks:

Extract Task:

Downloads the data files from the specified S3 buckets
(airports and airlines data) and the real-time flight data from the API.

Transform Task:

Performs necessary data transformations on the extracted data (e.g., cleaning, formatting, joining).

Load Task:

Loads the processed data into the PostgreSQL database hosted on AWS RDS and afterwards
to S3 Bucket.

CONCLUSION

This data engineering project demonstrates the creation of an automated data pipeline using Python, Airflow,
and AWS services. The pipeline extracts data from S3 and an API, performs transformations, and loads the
processed data into an AWS RDS PostgreSQL database. The focus of the project is to showcase skills in data
engineering and workflow management using Airflow.