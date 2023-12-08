# Databricks notebook source
race_results_df = spark.read.parquet(f'/mnt/formual1dl1122/presentation/race_results')

from pyspark.sql.functions import sum, count, col,when

constructor_standings_df = race_results_df.groupBy('race_year','team').agg(sum('points').alias('total_points'),count(when(col('position') == 1, True)).alias('wins'))

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

contructorRank = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
final_df = constructor_standings_df.withColumn("rank", rank().over(contructorRank))

# final_df.write.mode('overwrite').parquet(f'/mnt/formual1dl1122/presentation/constructor_standings')
final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation1.constructor_standings')

# COMMAND ----------

final_df.show()

# COMMAND ----------

