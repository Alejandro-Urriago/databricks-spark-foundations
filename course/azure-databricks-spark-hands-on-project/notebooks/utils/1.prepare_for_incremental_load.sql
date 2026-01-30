-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Drop all the tables

-- COMMAND ----------

DROP SCHEMA IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS f1_processed
MANAGED LOCATION 'abfss://processed@alejandroauedevdl.dfs.core.windows.net';

-- COMMAND ----------

DROP SCHEMA IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS f1_presentation 
MANAGED LOCATION 'abfss://presentation@alejandroauedevdl.dfs.core.windows.net';
