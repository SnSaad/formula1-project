-- Databricks notebook source
DROP DATABASE f1_processed CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
	LOCATION "/mnt/formual1dl1122/processed"

-- COMMAND ----------

DROP DATABASE f1_presentation CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
	LOCATION "/mnt/formual1dl1122/presentation"

-- COMMAND ----------

