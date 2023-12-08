# Databricks notebook source
from pyspark.sql import *


# COMMAND ----------

dbutils.widgets.text('p_file_date', '')
p_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

circuits_df = spark.read.json(f'/mnt/formual1dl1122/raw/{p_file_date}/results.json')

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')
from pyspark.sql.functions import lit

# COMMAND ----------

circuits_df=circuits_df.withColumn('data_source', lit(v_data_source)).withColumn('p_file_date', lit(p_file_date))

# COMMAND ----------

circuits_df = circuits_df.withColumnRenamed('constructorId','constructor_id').withColumnRenamed('driverId','driver_id').withColumnRenamed('raceId','race_id').withColumnRenamed('resultId','result_id').withColumnRenamed('statusId','status_id').withColumnRenamed('fastesLap','fastest_lap').withColumnRenamed('fastestLapTime','fastes_lap_time').withColumnRenamed('fastestLapSpeed','fastest_lap_speed').withColumnRenamed('positionOrder','position_order').withColumnRenamed('positionText','position_text')

# COMMAND ----------

circuits_df = circuits_df.drop('status_id')

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE f1_processed.results;

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

## Method1

# COMMAND ----------

# for race_id_list in circuits_df.select("race_id").distinct().collect():
#     spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# circuits_df.write.mode('overwrite').partitionBy('race_id').parquet('/mnt/formual1dl1122/processed/results')
# circuits_df.write.mode('overwrite').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results')
# circuits_df.write.mode('overwrite').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results')

# COMMAND ----------

## Method2

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_processed.results

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
    col_list=[]
    for col_name in input_df.schema.names:
        if col_name != partition_column:
            col_list.append(col_name)
        col_list.append(partition_column)
        output_df = input_df.select(col_list)
        return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    output_df = re_arrange_partition_column(input_df, partition_column)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode('overwrite').partitionBy(partition_column).format('parquet').saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

overwrite_partition(circuits_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

spark.read.parquet('/mnt/formuall1dl1122/processed/results/').show()

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

