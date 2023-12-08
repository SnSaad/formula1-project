-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- Create or replace the table in the f1_raw database
DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits (
    circuitId INT,
    circuitRef STRING,
    name STRING,
    location STRING,
    country STRING,
    lat DOUBLE,
    lng DOUBLE,
    alt INT,
    url STRING
)
USING csv
OPTIONS (
    path "/mnt/formual1dl1122/raw/circuits.csv",
    header true
);


-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

-- Drop the table if it exists
DROP TABLE IF EXISTS f1_raw.races;

-- Create the table races in the f1_raw database
CREATE TABLE IF NOT EXISTS f1_raw.races (
    raceId INT,
    year INT,
    round INT,
    circuitId INT,
    name STRING,
    date DATE,
    time STRING,
    url STRING
)
USING csv
OPTIONS (
    path "/mnt/formual1dl1122/raw/races.csv", -- Corrected path
    header true
);


-- COMMAND ----------

SELECT * FROM f1_raw.races

-- COMMAND ----------

DROP TABLE IF EXISTS fl_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationnality STRING,
url STRING)
USING json
OPTIONS(path "/mnt/formual1dl1122/raw/constructors.json")

-- COMMAND ----------

SELECT * FROM f1_raw.constructors

-- COMMAND ----------

--Constructor Table
DROP TABLE IF EXISTS fl_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationnality STRING,
url STRING)
USING json
OPTIONS(path "/mnt/formual1dl1122/raw/constructors.json");

--Driver Table
DROP TABLE IF EXISTS fl_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING)
USING json
OPTIONS(path "/mnt/formual1dl1122/raw/drivers.json");

--Results Table
DROP TABLE IF EXISTS fl_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestlap INT,
rank INT,
fastestlapTime STRING,
fastestlapSpeed FLOAT,
statusId STRING)
USING json
OPTIONS(path "/mnt/formual1dl1122/raw/results.json");

--Pit_stops Table
DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING)
USING json
OPTIONS( path "/mnt/formula1dl1122/raw/pit_stops.json", multiline true);

-- COMMAND ----------

--Multiple csv files
DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT)
USING json
OPTIONS( path "/mnt/formula1dl/raw/lap_times")


-- COMMAND ----------

--Multiple json files
DROP TABLE IF EXISTS fl_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT)
USING json
OPTIONS(path "/mnt/formual1dl/raw/qualifying", multiline true)

-- COMMAND ----------

