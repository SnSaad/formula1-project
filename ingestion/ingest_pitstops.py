# Databricks notebook source
from pyspark.sql import *

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

circuits_schema = StructType([
    StructField('raceId', IntegerType(),False),
    StructField('driverId', IntegerType(),True),
    StructField('stop',StringType(),True),
    StructField('lap',IntegerType(),True),
    StructField('time', StringType(),True),
    StructField('dob',DateType(),True),
    StructField('duration', DoubleType(),True),
    StructField('milliseconds', DoubleType(),True),
])

# COMMAND ----------

# circuits_df = spark.read \
#     .schema(circuits_schema) \
#     .option('multiline', True) \
#     .json('/mnt/formual1dl1122/raw/pit_stops.json')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '')
p_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

circuits_df=spark.read.json(f'/mnt/formual1dl1122/raw/{p_file_date}/pit_stops.json',multiLine=True)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')
from pyspark.sql.functions import lit


# COMMAND ----------

circuits_df=circuits_df.withColumn('data_source', lit(v_data_source)).withColumn('p_file_date',lit(p_file_date))

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

circuits_df = circuits_df.withColumn('ingestion_date',current_timestamp()).withColumnRenamed('raceId','race_id').withColumnRenamed('driverId','driver_id')

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

def overwrite_partition(output_df, db_name, table_name):
    # output_df = re_arrange_partition_column(input_df, partition_column)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode('overwrite').format('parquet').saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

overwrite_partition(circuits_df, 'f1_processed', 'pit_stops')

# COMMAND ----------

# circuits_df.write.mode('overwrite').parquet('/mnt/formual1dl1122/processed/pit_stops')
# circuits_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.pit_stps')

# COMMAND ----------

dbutils.fs.ls('/mnt/formulal1dl1122/processed/pit_stops/')

# COMMAND ----------

spark.read.parquet('/mnt/formulal1dl1122/processed/pit_stops/').show()

# COMMAND ----------

dbutils.notebook.exit('Success')
