# Databricks notebook source
# MAGIC %md
# MAGIC 1. Set the spark config fs_azure_account_key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

c = dbutils.secrets.get(scope='scopeformula', key='formula1-clientId')
t = dbutils.secrets.get(scope='scopeformula', key='formual1-tenantID')
s = dbutils.secrets.get(scope='scopeformula', key='formual1-secretKey')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formual1dl1122.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formual1dl1122.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formual1dl1122.dfs.core.windows.net", c)
spark.conf.set("fs.azure.account.oauth2.client.secret.formual1dl1122.dfs.core.windows.net", s)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formual1dl1122.dfs.core.windows.net", f"https://login.microsoftonline.com/{t}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formual1dl1122.dfs.core.windows.net")


# COMMAND ----------

spark.read.csv('abfss://demo@formual1dl1122.dfs.core.windows.net/circuits.csv',header=True)

# COMMAND ----------

