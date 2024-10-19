# Flight Data Pipeline Project

This project focuses on building a data pipeline on the cloud using a combination of tools to analyze flight delay data from the Kaggle dataset to build a dashboard in Google Looker Studio. The goal is to provide insights into flight delays based on various parameters such as origin, destination, airlines, and more.

## Project Overview

The end result of this project is a **Flight Delays Dashboard**, which provides a visual representation of key flight delay metrics such as average departure and arrival delays. The dashboard, as seen below, is hosted on Google Looker Studio, showing trends across multiple airports and time periods.


![Pipeline Arch](https://github.com/user-attachments/assets/a908dc1b-ea01-4def-a4ec-99fc77a3d552)


## Data Source

The flight delay data used in this project was sourced from Kaggle. You can access the dataset using the following link:

- Kaggle Flight Delay Data: [Kaggle Dataset Link](https://www.kaggle.com/datasets/robikscube/flight-delay-dataset-20182022)  <!-- Replace with actual Kaggle dataset link -->

## Project Workflow

The overall workflow of the project can be divided into the following steps:

### 1. Data Download and Preparation
- The flight delay data was downloaded from Kaggle.
- The data was partitioned by years and converted into **Parquet** format to optimize for storage and processing efficiency.

### 2. Data Storage in Google Cloud Storage
- The partitioned Parquet files were uploaded to **Google Cloud Storage** (GCS) for scalable storage and access.

### 3. Data Transformation with PySpark
- A **PySpark** job was created to perform data transformations on the raw data.
- The transformation pipeline was executed using **Google Dataproc**, a fully-managed cloud service for running Apache Spark and Hadoop clusters.

### 4. Data Warehouse with Google BigQuery
- The transformed data was loaded into **Google BigQuery**, a fully-managed data warehouse, where it was used to create a structured dataset for analysis.

### 5. Data Visualization with Google Looker Studio
- A **View** was created in BigQuery to aggregate the transformed flight data for the dashboard.
- The data was visualized using **Google Looker Studio**, providing a user-friendly interface for monitoring and analyzing flight delays based on selected filters (e.g., Origin, Destination, Airline, Date Range).
  
  Key metrics in the dashboard:
  - Average Departure Delay
  - Average Arrival Delay
  - Flight Delay Trends over Time

## Technologies Used

- **Google Cloud Storage (GCS)**: For storing raw and partitioned data.
- **Google Dataproc**: For transforming data using PySpark.
- **Google BigQuery**: As the data warehouse solution to store and manage the transformed data.
- **Google Looker Studio**: For building the final data visualization and dashboard.
- **PySpark**: For data transformation and processing.
- **Parquet**: As the file format for efficient storage.

## Visualization

Here are some snapshots of the final dashboard created in Google Looker Studio:

1. **Dashboard Overview**:
   <img width="899" alt="Dashboard ss 1" src="https://github.com/user-attachments/assets/08f4ea1d-ccad-4bea-9e26-4fc148a5bd27">

   <img width="902" alt="Dashboard ss 2" src="https://github.com/user-attachments/assets/a3fdc2e6-4959-42cd-8799-0cef28ab4ec9">


NOTE: Orchestration and Dockerization is still a WIP.
