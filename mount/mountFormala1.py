# Databricks notebook source
from pyspark.sql import *

# COMMAND ----------

# c = dbutils.secrets.get(scope='scopeformula', key="formula1-clientId")
# t = dbutils.secrets.get(scope='scopeformula', key="formual1-tenantID")
# s = dbutils.secrets.get(scope='scopeformula', key="formual1-secretKey")

# COMMAND ----------

# configs = {"fs.azure.account.auth.type": "OAuth",
# "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# "fs.azure.account.oauth2.client.id": c,
# "fs.azure.account.oauth2.client.secret": s,
# "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{t}/oauth2/token"}

# COMMAND ----------

# dbutils.fs.mount(
#     source="abfss://demo@formual1dl1122.dfs.core.windows.net",
#     mount_point="/mnt/formual1dl1122/demo",
#     extra_configs=configs
# )

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # Get secrets from Key Vault
    client_id = dbutils.secrets.get(scope='scopeformula', key="formula1-clientId")
    tenant_id = dbutils.secrets.get(scope='scopeformula', key="formual1-tenantID")
    client_secret = dbutils.secrets.get(scope='scopeformula', key="formual1-secretKey")
    
    # Set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": client_id,
              "fs.azure.account.oauth2.client.secret": client_secret,
              "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # Mount the storage account container
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('formual1dl1122', 'demo')
mount_adls('formual1dl1122', 'raw')
mount_adls('formual1dl1122', 'presentation')
mount_adls('formual1dl1122', 'processed')

# COMMAND ----------

