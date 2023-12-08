# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

lap_schema = StructType([
    StructField('qualifyId', IntegerType(),True),
    StructField('raceId', IntegerType(),True),
    StructField('driverId', IntegerType(),True),
    StructField('constructorId', IntegerType(),True),
    StructField('number', IntegerType(),True),
    StructField('position', IntegerType(),True),
    StructField('q1', StringType(),True),
    StructField('q2', StringType(),True),
    StructField('q3', StringType(),True),
])

# COMMAND ----------

dbutils.widgets.text('p_file_date', '')
p_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

lap_df=spark.read.schema(lap_schema).json(f'/mnt/formual1dl1122/raw/{p_file_date}/qualifying', multiLine=True)

# COMMAND ----------

lap_df.show()

# COMMAND ----------

lap_df.printSchema()

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')
from pyspark.sql.functions import lit

# COMMAND ----------

lap_df=lap_df.withColumn('data_source', lit(v_data_source)).withColumn('p_file_date',lit(p_file_date))

# COMMAND ----------

lap_df = lap_df.withColumnRenamed('raceId','race_id').withColumnRenamed('driverId','driver_id').withColumnRenamed('constructorId','constructor_id').withColumnRenamed('qualifyId','qualify_id')

# COMMAND ----------

lap_df = lap_df.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

lap_df.show()

# COMMAND ----------

def overwrite_partition(output_df, db_name, table_name):
    # output_df = re_arrange_partition_column(input_df, partition_column)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode('overwrite').format('parquet').saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

overwrite_partition(lap_df, 'f1_processed', 'qualifying')

# COMMAND ----------

# lap_df.write.mode('overwrite').parquet('/mnt/formual1dl1122/processed/qualifying')
# lap_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.qualifying')

# COMMAND ----------

dbutils.notebook.exit('Success')
