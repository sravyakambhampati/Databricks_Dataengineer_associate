-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Data governance model
-- MAGIC 
-- MAGIC Programatically Grant/revoke/deny permissions on data objects

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Syntax
-- MAGIC 
-- MAGIC Grant {privilege} on {Object} {object_name} to {user_group}
-- MAGIC 
-- MAGIC Privileges:
-- MAGIC * Select
-- MAGIC * Modify
-- MAGIC * Create
-- MAGIC * Read Metadata
-- MAGIC * Usage: Required to perform any action on database object
-- MAGIC * All Privileges
-- MAGIC 
-- MAGIC Object types:
-- MAGIC * Table
-- MAGIC * schema
-- MAGIC * Catalog
-- MAGIC * View
-- MAGIC * Function
-- MAGIC * any file : control access to underlying file system
-- MAGIC 
-- MAGIC 
-- MAGIC Catalog--> Schema--| Table,view,function
-- MAGIC 
-- MAGIC Grant, deny, revoke, show grants

-- COMMAND ----------


