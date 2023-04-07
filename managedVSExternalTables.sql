-- Databricks notebook source
-- MAGIC %md
-- MAGIC 1. What is database in Databricks?
-- MAGIC Database is actually a Schema in hive metastore
-- MAGIC 
-- MAGIC Syntax to create a database
-- MAGIC * Create Database dbname;
-- MAGIC * Create Schema dbname;
-- MAGIC Both syntaxes are same
-- MAGIC 
-- MAGIC 2. Tables:
-- MAGIC We have two kinds of tables Managed and external tables

-- COMMAND ----------

Create or replace table managed_default
(width INT, length INT, height INT);
insert into managed_default
VALUES (3, 2, 1)

-- COMMAND ----------

describe history managed_default

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/managed_default'

-- COMMAND ----------

describe extended managed_default

-- COMMAND ----------

show tables

-- COMMAND ----------

drop table managed_default

-- COMMAND ----------

describe extended managed_default

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('dbfs:/user/hive/warehouse/managed_default', recurse=True)

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/managed_default'

-- COMMAND ----------

create or replace table external_default
(width INT, length INT, height INT)
location 'dbfs:/mnt/demo/external_default';
insert into external_default
VALUES (3, 2, 1)

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_default'

-- COMMAND ----------

describe extended external_default

-- COMMAND ----------

drop table external_default

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_default'

-- COMMAND ----------

Create schema db;

-- COMMAND ----------

use db;

-- COMMAND ----------

create or replace table manged_db
(width INT, length INT, height INT);
insert into manged_db
VALUES (3, 2, 1);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('dbfs:/user/hive/warehouse/db.db/manged_db', recurse=True)

-- COMMAND ----------

describe extended manged_db;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/db.db/manged_db'

-- COMMAND ----------

drop table manged_db

-- COMMAND ----------

create schema custom
Location 'dbfs:/schema/custom.db';
Use custom;

-- COMMAND ----------

create or replace table external_custom
(width INT, length INT, height INT)
Location 'dbfs:/mnt/demo/external_custo';
insert into external_custom
VALUES (3, 2, 1);

-- COMMAND ----------

describe extended external_custom;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_custo'

-- COMMAND ----------

drop table external_custom;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.run("./Show_databases_tables", 60)

-- COMMAND ----------

-- MAGIC %run ./Show_databases_tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.help()

-- COMMAND ----------

describe extended external_custom

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_custom'

-- COMMAND ----------

create or replace table managed_custom
(width INT, length INT, height INT);
insert into managed_custom
VALUES (3, 2, 1);

-- COMMAND ----------

describe extended managed_custom

-- COMMAND ----------

drop table managed_custom

-- COMMAND ----------

-- MAGIC %run ./Show_databases_tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("dbfs:/schema/custom.db/managed_custom/")

-- COMMAND ----------

describe detail managed_custom

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/schema/custom.db/managed_custom"

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/schema/custom.db/"

-- COMMAND ----------

use custom;
drop table external_custom

-- COMMAND ----------

use default;
drop table external_default

-- COMMAND ----------


