# Databricks notebook source
v_result = dbutils.notebook.run('ingest_circuits',0,{'p_data_source': 'Ergast API','p_file_date': '2021-03-28'})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run('ingest_drivers',0,{'p_data_source': 'Ergast API','p_file_date': '2021-03-28'})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run('ingest_constructor',0,{'p_data_source': 'Ergast API','p_file_date': '2021-03-28'})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run('ingest_pitstops',0,{'p_data_source': 'Ergast API','p_file_date': '2021-03-28'})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run('ingest_laptimes',0,{'p_data_source': 'Ergast API','p_file_date': '2021-03-28'})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run('ingest_qualifying',0,{'p_data_source': 'Ergast API','p_file_date': '2021-03-28'})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run('ingest_races',0,{'p_data_source': 'Ergast API','p_file_date': '2021-03-28'})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run('ingest_results',0,{'p_data_source': 'Ergast API','p_file_date': '2021-03-28'})
v_result

# COMMAND ----------

