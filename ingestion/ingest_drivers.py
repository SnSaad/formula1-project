# Databricks notebook source
from pyspark.sql import *

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

ns = StructType([
    StructField('forename',StringType(),True),
    StructField('surname',StringType(),True),
])

# COMMAND ----------

circuits_schema = StructType([
    StructField('driverId', IntegerType(),False),
    StructField('driverRef', StringType(),True),
    StructField('number',IntegerType(),True),
    StructField('code',StringType(),True),
    StructField('name', ns),
    StructField('dob',DateType(),True),
    StructField('nationality', StringType(),True),
    StructField('url', StringType(),True),
])

# COMMAND ----------

dbutils.widgets.text('p_file_date', '')
p_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

circuits_df = spark.read \
    .option('header','true') \
    .schema(circuits_schema) \
    .json(f'/mnt/formual1dl1122/raw/{p_file_date}/drivers.json')

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')
from pyspark.sql.functions import lit

# COMMAND ----------

circuits_df=circuits_df.withColumn('data_source', lit(v_data_source))

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# circuits_df.select(circuits_df['circuitId']).show()

# COMMAND ----------

# circuits_df.select(circuits_df.circuitId).show()

# COMMAND ----------

# circuits_df.select('circuitId').show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

circuits_df=circuits_df.withColumn('ingestion_date', current_timestamp()).withColumn('name',concat(col("name.forename"), lit(" "), col("name.surname"))).withColumn('p_file_date', lit(p_file_date))

# COMMAND ----------

circuits_df=circuits_df.drop('url')

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

# circuits_df.write.mode('overwrite').parquet('/mnt/formual1dl1122/processed/drivers')
circuits_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.drivers')

# COMMAND ----------

dbutils.fs.ls('/mnt/formulal1dl1122/processed/drivers/')

# COMMAND ----------

dbutils.notebook.exit('Success')


# COMMAND ----------

