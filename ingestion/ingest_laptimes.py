# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

lap_schema = StructType([
    StructField('raceId', IntegerType(),True),
    StructField('driverId', IntegerType(),True),
    StructField('lap', IntegerType(),True),
    StructField('position', IntegerType(),True),
    StructField('time', StringType(),True),
    StructField('milliseconds', IntegerType(),True),
])

# COMMAND ----------

dbutils.widgets.text('p_file_date', '')
p_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

lap_df=spark.read.schema(lap_schema).csv(f'/mnt/formual1dl1122/raw/{p_file_date}/lap_times')

# COMMAND ----------

lap_df.show()

# COMMAND ----------

lap_df.printSchema()

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')
from pyspark.sql.functions import lit


# COMMAND ----------

lap_df=lap_df.withColumn('data_source', lit(v_data_source)).withColumn('p_file_date', lit(p_file_date))

# COMMAND ----------

lap_df = lap_df.withColumnRenamed('raceId','race_id').withColumnRenamed('driverId','driver_id')

# COMMAND ----------

lap_df = lap_df.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

lap_df.show()

# COMMAND ----------

lap_df.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_processed.lap_times

# COMMAND ----------

# lap_df.write.mode('overwrite').parquet('/mnt/formual1dl1122/processed/lap_times')
# lap_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.lap_times')

# COMMAND ----------

# def re_arrange_partition_column(input_df, partition_column):
#     col_list=[]
#     for col_name in input_df.schema.names:
#         if col_name != partition_column:
#             col_list.append(col_name)
#         col_list.append(partition_column)
#         output_df = input_df.select(col_list)
#         return output_df
def overwrite_partition(output_df, db_name, table_name):
    # output_df = re_arrange_partition_column(input_df, partition_column)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode('overwrite').format('parquet').saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

overwrite_partition(lap_df, 'f1_processed', 'lap_times')

# COMMAND ----------

dbutils.notebook.exit('Success')


# COMMAND ----------

