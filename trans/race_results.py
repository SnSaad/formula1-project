# Databricks notebook source
drivers_df = spark.read.parquet(f"/mnt/formual1dl1122/processed/drivers").withColumnRenamed('number','driver_number').withColumnRenamed('name','driver_name').withColumnRenamed('nationality','driver_nationality')

constructors_df = spark.read.parquet(f"/mnt/formual1dl1122/processed/constructors").withColumnRenamed('name','team')

circuits_df = spark.read.parquet(f"/mnt/formual1dl1122/processed/circuits").withColumnRenamed('location','circuit_location')

races_df = spark.read.parquet(f"/mnt/formual1dl1122/processed/races").withColumnRenamed('name','race_name').withColumnRenamed('race_timestamp','race_date')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '')
p_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

results_df = spark.read.parquet(f"/mnt/formual1dl1122/processed/results",inferSchema=True).withColumnRenamed('time','race_time')

    # # .filter(f"'p_file_date' = {p_file_date}")
    # .withColumnRenamed('time','race_time') \
    # # .withColumnRenamed('race_id','race_result_id')

# COMMAND ----------

circuits_df = circuits_df.withColumnRenamed('circuitId','circuit_id')

# COMMAND ----------

drivers_df = drivers_df.withColumnRenamed('driverId','driver_id').withColumnRenamed('driverRef','driver_ref')

# COMMAND ----------

constructors_df = constructors_df.withColumnRenamed('constructoId','constructor_id').withColumnRenamed('constructorRef','constructor_ref')

# COMMAND ----------

results_df.columns

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, 'inner') \
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

race_results_df = results_df.join(race_circuits_df, results_df.race_result_id == race_circuits_df.race_id).join(drivers_df, results_df.driverId == drivers_df.driver_id).join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

from pyspark.sql.functions import current_timestamp
final_df = race_results_df.select('race_year', 'race_name', 'race_date', 'circuit_location', 'driver_name', 'driver_number', 'driver_nationality','team','grid','fastestLap', 'race_time', 'points','position').withColumn('crated_data', current_timestamp())

# COMMAND ----------

display(final_df.filter("race_name == 'Abu Dhabi Grand Prix'"))

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_presentation.race_results

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
    col_list=[]
    for col_name in input_df.schema.names:
        if col_name != partition_column:
            col_list.append(col_name)
        col_list.append(partition_column)
        output_df = input_df.select(col_list)
        return output_df
def overwrite_partition(input_df, db_name, table_name, partition_column):
    output_df = re_arrange_partition_column(input_df, partition_column)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode('overwrite').partitionBy(partition_column).format('parquet').saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

overwrite_partition(final_df, 'f1_presentation', 'race_results','race_id')

# COMMAND ----------

# final_df.write.mode('overwrite').parquet(f'/mnt/formual1dl1122/presentation/race_results')
final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation1.race_results')

# COMMAND ----------

