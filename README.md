# NHS Workforce Medallion Pipeline - Databricks
This repository contains a Databricks data pipeline that ingests, cleans, and aggregates NHS Hospital and Community Health Services (HCHS) workforce data. The pipeline follows the Medallion Architecture to transform raw CSV turnover data into actionable "Gold" reporting tables.

Data sourced from https://digital.nhs.uk/data-and-information/publications/statistical/nhs-workforce-statistics
Using the monthly Turnover benchmarking - CSV data files

# Dataset Overview
The pipeline processes the NHS Workforce Statistics (Turnover Benchmarking) dataset.

Source: NHS England Digital
Grain: Organisation, Staff Group, and Reporting Period.
Key Metrics: Headcount (HC), Full-Time Equivalent (FTE), Joiners, and Leavers.

# Pipeline Architecture
  1. Bronze Layer (nhs_bronze_workforce)
    Input: Raw CSV files uploaded to cloud storage.
    Process: Uses Auto Loader (cloudFiles) to stream raw data into Delta format.
    Features: Schema evolution is enabled to catch unexpected source changes.

  2. Silver Layer (nhs_silver_workforce)
    Process: * Enforces strict schema types (Casting HC to Integer and FTE to Double).
    Standardizes column naming conventions (e.g., REPORTING_ORG_CODE).
    Handles "Rescued Data" from the ingest phase.
    Validation: Filters out malformed rows and ensures date consistency.

  3. Gold Layer (nhs_gold_pivoted_turnover)
    Process: * Pivots the TYPE column (Joiners, Leavers, Staff in Post) into distinct columns.
    Aggregates metrics by Period, Organisation, and Staff Group.
    Output: A "Wide" table optimized for BI tools (Power BI, Tableau) and SQL Dashboards.

# Setup & Usage
Prerequisites
A Databricks workspace (Standard or Premium).
Storage (ADLS Gen2, S3, or DBFS) for raw CSV files and checkpoints.
How to Run
Configure Notebooks: Import the notebooks into your Databricks workspace.
Set Checkpoint Path: Ensure the checkpoint_path variable in the Bronze notebook points to a persistent directory.
Deploy Workflow: Create a Databricks Job and chain the notebooks in the order: Bronze -> Silver -> Gold.
