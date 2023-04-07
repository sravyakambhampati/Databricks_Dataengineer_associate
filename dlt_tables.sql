-- Databricks notebook source
create or refresh streaming live table orders_raw
comment "The raw table of orders"
as select * from cloudFiles("dbfs:/mnt/demo-datasets/bookstore/orders-raw", "parquet", map("schema", "order_id STRING, order_timestamp LONG,customer_id STRING,quantity LONG"))

-- COMMAND ----------

create or refresh live table customers
as select * from json.`dbfs:/mnt/demo-datasets/bookstore/customers-json`

-- COMMAND ----------

create or refresh streaming live table orders_cleaned (
constraint valid_order_id expect (order_id is not null) on violation drop row)
comment "orders_cleaned"
as select o.order_id,quantity,c.customer_id,c.profile:first_name,c.profile:last_name,cast(from_unix(order_timestamp,"yyyy-MM-dd HH:mm:ss") as timestamp) order_timestamp from
stream(live.orders_raw) o left join live.customers c on o.customer_id=c.customer_id

-- COMMAND ----------

create or refresh live table daily_customer_books
comment "The gold layer"
as select customer_id,first_name,last_name,date_trun("DD",order_timestamp) as order_date,sum(quantity) books_count from stream(live.orders_cleaned) where country="china" group by 1,2,3,4

-- COMMAND ----------


