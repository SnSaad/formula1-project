# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text('p_file_date', '')
p_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

races_df = spark.read.csv(f'/mnt/formual1dl1122/raw/{p_file_date}/races.csv',header=True,inferSchema=True)

# COMMAND ----------

races_df.show()

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')
from pyspark.sql.functions import lit


# COMMAND ----------

races_df=races_df.withColumn('data_source', lit(v_data_source)).withColumn('p_file_date', lit(p_file_date))

# COMMAND ----------

races_df = races_df.select(col('raceId'),col('year'),col('round'),col('circuitId'),col('name'),col('date'),col('time'))

# COMMAND ----------



# COMMAND ----------

races_df = races_df.withColumnRenamed('raceId','race_id').withColumnRenamed('year','race_year').withColumnRenamed('circuitId','circuit_id')

# COMMAND ----------

races_df = races_df.withColumn('ingestion_date', current_timestamp()) \
    .withColumn('race_timestamp',to_timestamp(concat(col('date'), lit(' '), col('time'))))

# COMMAND ----------

races_df.show()

# COMMAND ----------

# races_df.write.mode('overwrite').parquet('/mnt/formual1dl1122/processed/races')
races_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.races')

# COMMAND ----------

dbutils.notebook.exit('Success')
