-- Databricks notebook source
-- MAGIC %md
-- MAGIC 1. Customers profile field to struct and use from_json(), shcema_of_json()

-- COMMAND ----------

-- MAGIC %run ./Copy-Datasets

-- COMMAND ----------

select * from customers

-- COMMAND ----------

describe customers

-- COMMAND ----------

select profile:first_name,profile:address:country from customers

-- COMMAND ----------

select profile from customers limit 1

-- COMMAND ----------

create or replace temp view parsed_customers
as select customer_id,from_json(profile, schema_of_json('{"first_name":"Thomas","last_name":"Lane","gender":"Male","address":{"street":"06 Boulevard Victor Hugo","city":"Paris","country":"France"}}')) as profile_struct from customers;

-- COMMAND ----------

select * from parsed_customers

-- COMMAND ----------

select profile_struct.first_name,profile_struct.address, profile_struct.address.country from parsed_customers

-- COMMAND ----------

create or replace temp view customers_final
as select customer_id,profile_struct.*
from parsed_customers;

select * from customers_final;

-- COMMAND ----------

select * from orders

-- COMMAND ----------

select order_id,customer_id, explode(books) books from orders;

-- COMMAND ----------

select customer_id,collect_set(order_id), collect_set(books.book_id) from orders group by customer_id;

-- COMMAND ----------

select customer_id,collect_set(order_id),array_distinct(flatten(collect_set(books.book_id))) from orders group by customer_id;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo-datasets/bookstore/'

-- COMMAND ----------

create temp view books_csv
(book_id string, title string, author string, category string,price int)
using csv
options(header="true",delimiter=';',path='dbfs:/mnt/demo-datasets/bookstore/books-csv')

-- COMMAND ----------

select * from books_csv

-- COMMAND ----------

create or replace table books
as select distinct * from books_csv

-- COMMAND ----------

select * from books

-- COMMAND ----------

select * from orders

-- COMMAND ----------

create or replace view orders_enriched
as
select * from (select *, explode(books) as book from orders) o inner join books b on o.book.book_id=b.book_id;

-- COMMAND ----------

select * from orders_enriched

-- COMMAND ----------

create or replace temp view orders_updates
as select * from parquet.`dbfs:/mnt/demo-datasets/bookstore/orders-new`

-- COMMAND ----------

select count(*) from orders

-- COMMAND ----------

select count(*) from (select * from orders
union select * from orders_updates) a;

-- COMMAND ----------

select count(*) from orders_updates

-- COMMAND ----------

select count(*) from (
select * from orders
minus
select * from orders_updates)

-- COMMAND ----------

select count(*) from (
select * from orders
intersect
select * from orders_updates)

-- COMMAND ----------

select * from orders_enriched

-- COMMAND ----------

select customer_id,book.book_id,book.quantity quantity from orders_enriched

-- COMMAND ----------

create or replace table transactions as
select * from (select customer_id,book.book_id,book.quantity quantity from orders_enriched) PIVOT (sum(quantity) for book_id in ('B01', 'B02', 'B03', 'B04', 'B05', 'B06',
    'B07', 'B08', 'B09', 'B10', 'B11', 'B12'));

-- COMMAND ----------

select * from transactions

-- COMMAND ----------

select * from orders

-- COMMAND ----------

select * from (select order_id,customer_id,Filter(books,i->i.quantity>=2) many_books from orders) b where size(b.many_books)>0;

-- COMMAND ----------

select order_id,customer_id,books.subtotal before_discount,transform(books, i->cast(i.subtotal*0.8 as int)) as subtotal_after_transform from orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # UDFS

-- COMMAND ----------

create or replace function get_url(email string)
returns string
return concat("https://www.",split(email,"@")[1])

-- COMMAND ----------

select email,get_url(email) as domain from customers

-- COMMAND ----------

create or replace function kindof(email string)
returns string
return case when email like '%.com' then "commercial business"
            when email like '%.org' then "Non profit organization"
            when email like '%.edu' then "Educational sites"
            else concat("unknow domain :",split(email,"@")[1])
       end;

-- COMMAND ----------

select email,kindof(email) from customers;

-- COMMAND ----------

drop function get_url;
drop function kindof

-- COMMAND ----------


