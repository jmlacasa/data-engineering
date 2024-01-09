# Data Engineering Course - Exercises & Projects

Welcome to the exercises and projects repository for the Data Engineering course at Coderhouse! This repository contains hands-on assignments, code samples, and projects written by me that are part of the course curriculum.

# Table of Contents

- [Introduction](#introduction)
- [Projects](#projects)
    - [ETL pipeline - 1st deliverable](#etl-pipeline---1st-deliverable)
    - [Staging Data and Validating - 2nd deliverable](#staging-data-and-validating---2nd-deliverable)
    - [Containerizing and Scheduling the ETL - 3rd deliverable](#containerizing-and-scheduling-the-etl---3rd-deliverable)
    - [ETL pipeline - 4th deliverable](#etl-pipeline---1st-deliverable)
- [Exercises](#exercises)
    - [Week 4 - Database Initialization and Data Processing Procedures](#week-4)
    - [Week 6 - Data Consumption and Trend Visualization](#week-6)

# Introduction

Welcome to the Data Engineering course project repository. 

# **Projects**

## ETL pipeline - 1st deliverable
[This project](./1st%20deliverable) revolves around the ETL (Extract, Transform, Load) process, focusing specifically on financial data - extracting stock ticker data, transforming it for analytical purposes, and eventually loading it into a data warehouse. Our final aim is to create a robust pipeline that can handle vast amounts of financial data, ensuring that it is readily available for any subsequent analysis.

The main technologies and tools we're employing include:
- **Pandas**: For scripting and data manipulation.
- **Amazon Redshift / PostgreSQL**: As our primary data storage and querying solution.
- **psycopg2**: To connect and interact with Amazon Redshift.
- **YFinance**: A data source utilized for fetching stock ticker data.

## Staging Data and Validating - 2nd deliverable
[This deliverable](./2nd%20deliverable) advances the ETL pipeline by adding a validation step on the downloaded data and uploading to a staging table before replacing in the production table.

The main technologies and tools we're employing include (additional to the ones mentioned in deliverable 1):
- **pandas_market_calendars**: for market data validation.
- **Custom functions built in Python**: making the repeatable process of uploading data replicable.

## Containerizing and Scheduling the ETL - 3rd deliverable
[On this deliverable](./3rd%20deliverable) a custom Docker compose is built for scheduling the ETL on Airflow. From this step onward the ETL is runnable on any computer that has Docker without any further configuration.

The main technologies and tools we're employing include (additional to the ones mentioned in deliverable 1 and 2):
- **Docker**: custom Docker compose file for further modularization and reproducibility.
- **Airflow**: daily execution of the ETL.
- **Papermill**: PapermillOperator for executing python notebooks.
- **PostgreSQL**: as a microservice in Docker with ticker data. (this can be replaced with Redshift connection information)

## Setting up XCOM and SMTP communication - 4th deliverable
[Here we set up](./4th%20deliverable%20(final)) XCOM between tasks in Airflow to forward basic data downloading reports to a new task. This tasks sends the message to specific receivers.

The main technologies and tools we're employing include (additional to the ones mentioned in deliverable 1, 2 and 3):
- **XCOM**: custom Docker compose file for further modularization and reproducibility.
- **SMTPlib**: python library to handle SMTP connections.

# **Exercises**
Weekly content has been selectively curated, omitting certain weeks with introductory material to maintain the specialized focus of this repository. 

## **Week 4**
### Database Initialization and Data Processing Procedures
The provided SQL scripts serve as the backbone for initializing and processing data within our `DESASTRES` database. The [`create_base.sql`](./week4/create_base.sql) script establishes the foundational structure of our database. It focuses on the creation of the `DESASTRES` database and subsequently sets up the `clima` table, designed to store climate-related data such as yearly temperature and oxygen levels.

On the other hand, the [`create_procedure.sql`](./week4/create_procedure.sql) script introduces a stored procedure, `petl_desastres()`, within the `public` schema. This procedure is pivotal for data processing and transformation. It ensures data consistency by first clearing entries in the `desastres_final` table and then populates it with aggregated data, emphasizing key metrics like average temperature, average oxygen levels, and total tsunamis over specified intervals.


## **Week 6**
### Data Consumption and Trend Visualization
This collection of projects encompasses various aspects of data handling and analysis. We begin with exploratory data analysis (EDA) in [`pandas_ex.ipynb`](./week6/pandas_ex.ipynb), focusing on assessing duplicates, null values, and anomalies in the dataset. This is followed by [`pytrend_ex.ipynb`](./week6/pytrend_ex.ipynb), where we delve into trend analysis using Google Trends data for specific keywords and visualizing the trend insights. Lastly, in [`sql-json_ex.ipynb`](./week6/sql-json_ex.ipynb), we extract data from both JSON and SQL sources, making it ready for further processing and analysis.

The main technologies and tools employed across these projects include:
- **Pandas**: Employed extensively for data manipulation and EDA.
- **Pytrends**: Utilized to fetch Google Trends data.
- **JSON**: To read and normalize raw and nested JSON files.
- **SQLite3**: For connecting to SQLite databases and extracting data.
