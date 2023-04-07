-- Databricks notebook source
-- MAGIC %md
-- MAGIC 1. create orders(parquet) delta table
-- MAGIC 2. Advantages of overwriting the data in the table 
-- MAGIC 3. Practice create or replace table, and insert overwrite
-- MAGIC 4. Merge into

-- COMMAND ----------

-- MAGIC %run ./Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{dataset_bookstore}/"))

-- COMMAND ----------

create or replace table orders
as select * from parquet.`${dataset.bookstore}/orders/`;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can accomplish complete overwrite using Create or replace table; This statement fully replace data every time this statement executes
-- MAGIC 
-- MAGIC Second way to overwrite data is to use Insert overwrite

-- COMMAND ----------

select count(*) from orders

-- COMMAND ----------

insert overwrite orders
select * from parquet.`${dataset.bookstore}/orders/`;

-- COMMAND ----------

-- MAGIC %md Difference b/w create or replace and Insert overwite
-- MAGIC 
-- MAGIC 1. Insert overwrite can only overwrite if table exists unlike create or replace creating new one
-- MAGIC 2. The way how they enforce schema on write is the basic difference between Insert overwrite and cRAS. 

-- COMMAND ----------

describe history orders

-- COMMAND ----------

select * from parquet.`dbfs:/mnt/demo-datasets/bookstore/orders-new/`;

-- COMMAND ----------

Insert into orders
select * from parquet.`dbfs:/mnt/demo-datasets/bookstore/orders-new/`;

-- COMMAND ----------

select count(*) from orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Insert into statement does not guarantee non duplicates. Executing same query results in duplicates

-- COMMAND ----------

select * from json.`dbfs:/mnt/demo-datasets/bookstore/customers-json-new/`

-- COMMAND ----------

create temp view customer_updates
as select * from json.`dbfs:/mnt/demo-datasets/bookstore/customers-json-new/`;

-- COMMAND ----------

merge into customers c
using customer_updates u
on c.customer_id=u.customer_id
when matched and c.email is null and u.email is not null then
update set c.email=u.email, c.updated=u.updated 
when not matched then insert *

-- COMMAND ----------

select * from csv.`dbfs:/mnt/demo-datasets/bookstore/books-csv-new/`

-- COMMAND ----------

create or replace temp view new_books
(book_id String, title String, author String, category String, price Double)
using csv
options(path="dbfs:/mnt/demo-datasets/bookstore/books-csv-new/", header=True,delimiter=';');

-- COMMAND ----------

select * from new_books

-- COMMAND ----------

select * from books

-- COMMAND ----------

merge into books_delta b
using new_books n
on b.book_id= n.book_id and b.title=n.title
when not matched and n.category='Computer Science'
then insert *

-- COMMAND ----------

create table books_delta
as select * from books;

-- COMMAND ----------

describe extended books_delta

-- COMMAND ----------


