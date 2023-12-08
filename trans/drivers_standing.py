# Databricks notebook source
race_results_df = spark.read.parquet(f'/mnt/formual1dl1122/presentation/race_results')

from pyspark.sql.functions import sum, count, when, col

driver_standings_df = race_results_df.groupBy('race_year','driver_name','driver_nationality','team').agg(sum('points').alias('total_points'),count(when(col('position') == 1, True)).alias('wins'))

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driverRank = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
final_df = driver_standings_df.withColumn("rank", rank().over(driverRank))

# COMMAND ----------

# final_df.write.mode('overwrite').parquet('/mnt/formual1dl1122/presentation/driver_standings')
final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation1.driver_standings')


# COMMAND ----------

