# Databricks notebook source
from pyspark.sql import *

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

circuits_schema = StructType([
    StructField('constructorId', IntegerType(),False),
    StructField('constructorRef', StringType(),True),
    StructField('name', StringType(),True),
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
    .json(f'/mnt/formual1dl1122/raw/{p_file_date}/constructors.json')

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

circuits_selected_df = circuits_df.select(col('constructorId'),col('constructorRef'),col('name'),col('nationality'))

# COMMAND ----------

circuits_selected_df=circuits_selected_df.withColumnRenamed('constructorId','constructor_id').withColumnRenamed('constructorRef','constructor_ref')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_selected_df=circuits_selected_df.withColumn('ingestion_date', current_timestamp()).withColumn('p_file_date', lit(p_file_date))

# COMMAND ----------

circuits_selected_df.show()

# COMMAND ----------

# circuits_selected_df.write.mode('overwrite').parquet('/mnt/formual1dl1122/processed/constructors')
circuits_selected_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.constructors')

# COMMAND ----------

dbutils.fs.ls('/mnt/formulal1dl1122/processed/constructors/')

# COMMAND ----------

dbutils.notebook.exit('Success')


# COMMAND ----------

