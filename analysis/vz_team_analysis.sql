-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW v_team
AS
SELECT race_year,
  team_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_points,
  rank() OVER (ORDER BY avg(calculated_points) DESC) as driver_rank
FROM f1_presentation1.calculated_results
GROUP BY race_year,team_name
ORDER BY race_year,avg_points DESC

-- COMMAND ----------

SELECT race_year,
  team_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_points,
  rank() OVER (ORDER BY avg(calculated_points) DESC) as driver_rank
FROM f1_presentation1.calculated_results
WHERE team_name IN (SELECT team_name FROM v_team WHERE driver_rank <=10)
GROUP BY race_year,team_name
ORDER BY race_year,avg_points DESC

-- COMMAND ----------

SELECT race_year,
  team_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_points,
  rank() OVER (ORDER BY avg(calculated_points) DESC) as driver_rank
FROM f1_presentation1.calculated_results
WHERE team_name IN (SELECT team_name FROM v_team WHERE driver_rank <=10)
GROUP BY race_year,team_name
ORDER BY race_year,avg_points DESC

-- COMMAND ----------

