-- Databricks notebook source
-- MAGIC %md
-- MAGIC 1. Querying data files directly
-- MAGIC 2. Extract files as raw contents
-- MAGIC 3. Configure options of external sources
-- MAGIC 4. Use CTAS to create delta tables

-- COMMAND ----------

-- MAGIC %run ./Copy-Datasets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Querying from files syntax
-- MAGIC 
-- MAGIC Select * from {file format like json,parquet,csv}.\`{Path to the file/directory}\`;
-- MAGIC 
-- MAGIC Self-describing formats:
-- MAGIC * Json
-- MAGIC * PArquet
-- MAGIC 
-- MAGIC Non self-decribing formats:
-- MAGIC * CSV
-- MAGIC * TSV
-- MAGIC * text

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dataset_bookstore

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo-datasets/bookstore/customers-json/'

-- COMMAND ----------

select * from json.`${dataset_bookstore}/customers-json/export_001.json`

-- COMMAND ----------

SELECT count(*) FROM json.`${dataset.bookstore}/customers-json/`

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json/`

-- COMMAND ----------

select *,input_file_name()
from json.`${dataset.bookstore}/customers-json/`;

-- COMMAND ----------

SELECT * FROM text.`${dataset.bookstore}/customers-json/export_001.json`

-- COMMAND ----------

SELECT * FROM binaryFile.`${dataset.bookstore}/customers-json/export_001.json`

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo-datasets/bookstore/books-csv/'

-- COMMAND ----------

select * from csv.`${dataset.bookstore}/books-csv/export_001.csv`

-- COMMAND ----------

create table books
(book_id string, title string, author string, category string,price int)
using csv
options(header="true", delimiter=';')
location "${dataset.bookstore}/books-csv/";

-- COMMAND ----------

describe extended books

-- COMMAND ----------

-- MAGIC %md
-- MAGIC When working with CSV files as datasource, it is important to ensure that column names, and datatypes are in order specified during creation of table
-- MAGIC 
-- MAGIC Create table location will create a external table and not delta format table. 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{dataset_bookstore}/books-csv/"))

-- COMMAND ----------

select * from books

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.table("books").write.mode("append").format("csv").option("header","true").option("delimiter",';').save(f"{dataset_bookstore}/books-csv/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{dataset_bookstore}/books-csv/"))

-- COMMAND ----------

select count(*) from books

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Spark automatically auto cache the underlying data in local storage to ensure that on subsequent queries spark provide optimal performance by reading directly from this cache
-- MAGIC This external csv is not configured to tell spark that it should refresh the data

-- COMMAND ----------

refresh table books;

-- COMMAND ----------

select count(*) from books

-- COMMAND ----------

create table if not exists customers
as select * from json.`${dataset.bookstore}/customers-json/`;

select * from customers;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/customers", recurse=True)

-- COMMAND ----------

describe extended customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CTAS is useful to injest data from sources like json and parquet
-- MAGIC 
-- MAGIC For CSV datasource here are the steps to create a delta table
-- MAGIC 
-- MAGIC 1. create a temp view from csv files/directory. Parse them using options
-- MAGIC 2. Then create a delta table out of this temp view

-- COMMAND ----------

create table books_unparsed
as select * from csv.`${dataset.bookstore}/books-csv`;
describe extended books_unparsed

-- COMMAND ----------

select * from books_unparsed;

-- COMMAND ----------

create temp view books_parsed_tmp
(book_id string, title string, author string, category string,price int)
using csv
options(header="true",delimiter=';',path="${dataset.bookstore}/books-csv/")

-- COMMAND ----------

describe extended books_parsed_tmp

-- COMMAND ----------

create table books_csv
as select * from books_parsed_tmp;

describe extended books_csv;

-- COMMAND ----------


