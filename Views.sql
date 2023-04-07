-- Databricks notebook source
-- MAGIC %md
-- MAGIC Databricks supports three types views
-- MAGIC 1. Stored Views: create view
-- MAGIC 2. Temporary Views: create temp view
-- MAGIC 3. Global Temporary Views: create global temp view

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Difference between stored views, temporary view and global views
-- MAGIC | Stored | Temp view| Global Temp Views|
-- MAGIC |--------|----------|------------------|
-- MAGIC |Persisted in databases; Dropped only by drop view command|Session Scoped; dropped when session ends| Cluster scoped; Drops when cluster restarted|

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS smartphones
(id INT, name STRING, brand STRING, year INT);

INSERT INTO smartphones
VALUES (1, 'iPhone 14', 'Apple', 2022),
      (2, 'iPhone 13', 'Apple', 2021),
      (3, 'iPhone 6', 'Apple', 2014),
      (4, 'iPad Air', 'Apple', 2013),
      (5, 'Galaxy S22', 'Samsung', 2022),
      (6, 'Galaxy Z Fold', 'Samsung', 2022),
      (7, 'Galaxy S9', 'Samsung', 2016),
      (8, '12 Pro', 'Xiaomi', 2022),
      (9, 'Redmi 11T Pro', 'Xiaomi', 2022),
      (10, 'Redmi Note 11', 'Xiaomi', 2021)

-- COMMAND ----------

show tables

-- COMMAND ----------

create temp view apple_phones
as select * from smartphones
where brand='Apple';

-- COMMAND ----------

select * from apple_phones

-- COMMAND ----------

create global temp view latest_apple_phones
as select * from apple_phones where year>=2021;

-- COMMAND ----------

select * from global_temp.latest_apple_phones

-- COMMAND ----------

show tables

-- COMMAND ----------


