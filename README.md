# Data Engineering Course - Exercises & Projects

Welcome to the exercises and projects repository for the Data Engineering course at Coderhouse! This repository contains hands-on assignments, code samples, and projects written by me that are part of the course curriculum.

# Table of Contents

- [Introduction](#introduction)
- [Projects](#projects)
    - [ETL pipeline - 1st deliverable](#etl-pipeline---1st-deliverable)
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
- **Amazon Redshift**: As our primary data storage and querying solution.
- **psycopg2**: To connect and interact with Amazon Redshift.
- **YFinance**: A data source utilized for fetching stock ticker data.


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
