# Databricks notebook source
from pyspark.sql import *

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

circuits_schema = StructType([
    StructField('circuitId', IntegerType(),False),
    StructField('circuitRef', StringType(),True),
    StructField('name', StringType(),True),
    StructField('location', StringType(),True),
    StructField('country', StringType(),True),
    StructField('lat', DoubleType(),True),
    StructField('lng', DoubleType(),True),
    StructField('alt', IntegerType(),True),
    StructField('url', StringType(),True),
])

# COMMAND ----------

dbutils.widgets.text('p_file_date', '')
p_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

circuits_df = spark.read \
    .option('header','true') \
    .schema(circuits_schema) \
    .csv(f'/mnt/formual1dl1122/raw/{p_file_date}/circuits.csv')

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

circuits_selected_df = circuits_df.select(col('circuitId'),col('circuitRef'),col('name'),col('location'),col('country'),col('lat'),col('lng'),col('alt'))

# COMMAND ----------

circuits_selected_df=circuits_selected_df.withColumnRenamed('cicuitId','circuit_id').withColumnRenamed('circuitRef','circuit_ref').withColumnRenamed('lat','latitude').withColumnRenamed('lng','longitude').withColumnRenamed('alt','altitude').withColumn('p_file_date', lit(p_file_date))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_selected_df=circuits_selected_df.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

circuits_selected_df.show()

# COMMAND ----------

# circuits_selected_df.write.mode('overwrite').parquet('/mnt/formual1dl1122/processed/circuits')
#Write the DataFrame circuits_selected_df as a Parquet table to the f1_processed database
circuits_selected_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.circuits');


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

