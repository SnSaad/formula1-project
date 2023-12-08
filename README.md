# Formula 1 Project Using Microsoft Azure

This repository contains a Formula 1 project demonstrating the integration of various Microsoft Azure services for data management, transformation, and analysis.

## Project Overview
This comprehensive project focuses on Formula 1 (F1) data ingestion from Azure Data Lake into Azure Databricks, leveraging PySpark for intricate data transformations within the Databricks environment. The transformed data is loaded into an SQL database within Databricks, enabling seamless visualization and analysis using PySpark SQL for generating insightful visualizations and reports entirely within the Databricks platform. This end-to-end workflow emphasizes the power of PySpark and SQL for managing, transforming, and visualizing F1 data in a unified Databricks environment, facilitating efficient and insightful data-driven decision-making processes.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Setup](#setup)
- [Data Ingestion](#data-ingestion)
- [Data Transformation using Data Flow and Data Bricks](#data-transformation-using-data-flow-and-data-bricks)
- [Data Analysis](#data-analysis)
- [Results](#results)
- [Contributing](#contributing)


## Prerequisites

- **Azure Key Vault**:
- **Azure Data Factory (ADF)**: Ensure access to and familiarity with Azure Data Factory for orchestrating data workflows, including data ingestion from various sources into Azure services.
- **Azure Databricks (ADB)**: Familiarity with Azure Databricks is essential for data transformation tasks, as it provides a collaborative Apache Spark-based analytics platform for processing large-scale data.
- **Azure SQL Database (ASQL)**: Proficiency in setting up and managing Azure SQL Database, as it serves as the storage solution for processed data in this project.
- **Azure Subscription**: Access to an active Azure subscription is required to utilize the Azure services (ADF, ADB, ASQL) and deploy resources necessary for data management and analysis.
- **Power BI**: Basic understanding of Power BI for data visualization and reporting purposes, as the project integrates Power BI to create insightful dashboards and reports based on the transformed data from Azure services.

### Project Structure

The project is organized into the following sections:
### 1. Data Ingestion - Eragast API to Data Lake
- Utilize Eragast API for fetching relevant data.
- Ingest acquired data into Azure Data Lake for storage and further processing.
### - Data Lake to Databricks
- Transfer data from Azure Data Lake to Azure Databricks for subsequent transformations and analysis.
### 2. Data Transformation - PySpark Data Transformation
- Use PySpark within Databricks for comprehensive data transformation, leveraging its capabilities for processing and refining the ingested data from the Data Lake.
- Apply various transformations and cleansing operations as required.
### 3. SQL Database Creation - SQL Database and Table Creation
- Establish an SQL database within Databricks.
- Create appropriate tables for storing the transformed data, organizing it for analysis and visualization.
### 4. Data Visualization and Insights - Visualization using PySpark SQL
- Leverage PySpark SQL capabilities within Databricks for data visualization and analysis.
- Create insightful visualizations and reports to extract meaningful insights from the transformed data.

### Setup

1. Create an Azure Account
2. Create Azure Key Vault
3. Create Microsoft Entra ID
4. Create an Azure Data Lake Storage Gen2 account.
5. Creat an Azure SQL Database
6. Configure an Azure Data Bricks workspace.
7. Prepare your development environment, including installing required libraries.

### Data Ingestion from Eragast API to Azure Data Lake and Databricks
### Overview
This project focuses on data ingestion from the Eragast API, storing the obtained datasets in Azure Data Lake Storage, and accessing this data within Azure Databricks. The process involves downloading datasets related to circuits, constructors, drivers, pit stops, lap times, race results, and qualifying information from the Eragast API and subsequently organizing them in Azure Data Lake Storage. Additionally, it establishes secure credential storage using Azure Key Vault and access configuration to the Data Lake from Azure Databricks.
![Screenshot (97)](https://github.com/SnSaad/formula1-project/assets/98678581/a468ff3d-b2f9-4f85-9f6c-5591d8a9ccde)
![Screenshot (98)](http![Screenshot (99)](https://github.com/SnSaad/formula1-project/assets/98678581/9c414ccd-c4b6-41db-bc05-6c8f40f5147c)
s://github.com/SnSaad/formula1-project/assets/98678581/04b7a972-7bde-423c-944f-28b6f5aab7e1)
![Screenshot (100)](https://github.com/SnSaad/formula1-project/assets/98678581/0ec88c00-d41e-474d-90cb-e6e2fa725a3c)

### Steps

1. **Downloading Datasets from Eragast API**:
   - Retrieve datasets related to circuits, constructors, drivers, pit stops, lap times, race results, and qualifying from the Eragast API.

2. **Storing Datasets in Azure Data Lake**:
   - Upload acquired datasets into Azure Data Lake Storage, organizing them within the 'raw' folder for further processing.

3. **Setting up Azure Key Vault for Credential Storage**:
   - Create a Microsoft Enterprise Application named 'formual1-app' to manage secure credentials (client ID, tenant ID, secret) in Azure Key Vault.

4. **Providing Data Lake Access to 'formual1-app'**:
   - Grant 'formual1-app' appropriate access permissions to the Azure Data Lake Storage for secure and controlled data retrieval.

5. **Configuring Secret Scope in Azure Databricks**:
   - Establish a secret scope in Azure Databricks linked to the Azure Key Vault, allowing access to the stored keys and credentials.

6. **Accessing Data Lake from Azure Databricks**:
   - Use the configured secret scope to access and mount the 'raw' folder within Azure Data Lake in Azure Databricks for further processing and analysis.





