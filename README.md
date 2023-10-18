# Data Engineering Course - Exercises & Projects

Welcome to the exercises and projects repository for the Data Engineering course at Coderhouse! This repository contains hands-on assignments, code samples, and projects written by me that are part of the course curriculum.

## Table of Contents

- [Introduction](#introduction)
- [Projects](#projects)
    - [ETL pipeline - 1st deliverable](#etl-pipeline---1st-deliverable)

## Introduction

Welcome to the Data Engineering course project repository. 

## Projects

### ETL pipeline - 1st deliverable
[This project](./1st%20deliverable) revolves around the ETL (Extract, Transform, Load) process, focusing specifically on financial data - extracting stock ticker data, transforming it for analytical purposes, and eventually loading it into a data warehouse. Our final aim is to create a robust pipeline that can handle vast amounts of financial data, ensuring that it is readily available for any subsequent analysis.

The main technologies and tools we're employing include:
- **Python**: For scripting and data manipulation using libraries such as `pandas`.
- **Amazon Redshift**: As our primary data storage and querying solution.
- **psycopg2**: To connect and interact with Amazon Redshift.
- **YFinance**: A data source utilized for fetching stock ticker data.

