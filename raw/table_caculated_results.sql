-- Databricks notebook source
USE f1_processed1

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE TABLE f1_presentation1.calculated_results
USING PARQUET
AS
SELECT races.race_year,
      constructors.name AS team_name,
      drivers.name AS driver_name,
      results.position,
      results.points,
      11 - results.position AS calculated_points
FROM f1_processed1.results
JOIN f1_processed1.drivers ON (f1_processed1.results.driver_id==f1_processed1.drivers.driverId)
JOIN f1_processed1.constructors ON (results.constructor_id==constructors.constructor_id)
JOIN f1_processed1.races ON (results.race_id==races.race_id)
WHERE results.position <=10

-- COMMAND ----------

SELECT * FROM f1_presentation1.calculated_results

-- COMMAND ----------

