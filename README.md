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


### Prerequisites

- **Azure Key Vault**:
- **Azure Data Factory (ADF)**: Ensure access to and familiarity with Azure Data Factory for orchestrating data workflows, including data ingestion from various sources into Azure services.
- **Azure Databricks (ADB)**: Familiarity with Azure Databricks is essential for data transformation tasks, as it provides a collaborative Apache Spark-based analytics platform for processing large-scale data.
- **Azure SQL Database (ASQL)**: Proficiency in setting up and managing Azure SQL Database, as it serves as the storage solution for processed data in this project.
- **Azure Subscription**: Access to an active Azure subscription is required to utilize the Azure services (ADF, ADB, ASQL) and deploy resources necessary for data management and analysis.
- **Power BI**: Basic understanding of Power BI for data visualization and reporting purposes, as the project integrates Power BI to create insightful dashboards and reports based on the transformed data from Azure services.


