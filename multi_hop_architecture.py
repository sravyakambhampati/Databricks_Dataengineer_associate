# Databricks notebook source
# MAGIC %run ./Copy-Datasets

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/mnt/demo-datasets/bookstore/orders-raw'

# COMMAND ----------

spark.readStream.format("cloudFiles").option("cloudFiles.format","parquet").option("cloudFiles.schemaLocation","dbfs:/mnt/demo/checkpoint/orders_raw").load("dbfs:/mnt/demo-datasets/bookstore/orders-raw").createOrReplaceTempView("orders_raw_tmp")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view orders_tmp as
# MAGIC select *,current_timestamp() arrival_time,input_file_name() source_file from orders_raw_tmp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_tmp

# COMMAND ----------

spark.table("orders_tmp").writeStream.format("delta").option("checkpointLocation","dbfs:/mnt/demo/checkpoints/bronze").outputMode("append").table("orders_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_bronze

# COMMAND ----------

load_new_data()

# COMMAND ----------

spark.read.format("json").load("dbfs:/mnt/demo-datasets/bookstore/customers-json").createOrReplaceTempView("customers_lookup")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers_lookup

# COMMAND ----------

spark.readStream.table("orders_bronze").createOrReplaceTempView("orders_bronze_tmp")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view orders_enriched_temp_view as
# MAGIC (select order_id,quantity,o.customer_id,c.profile:firstname,c.profile:lastname, cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') as timestamp) order_timestamp,books from orders_bronze_tmp o inner join customers_lookup c on o.customer_id=c.customer_id where quantity>0)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_enriched_temp_view

# COMMAND ----------

spark.table("orders_enriched_temp_view").writeStream.format("delta").option("checkpointLocation","dbfs:/mnt/demo/checkpoints/silver").outputMode("append").table("orders_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_silver

# COMMAND ----------

load_new_data()

# COMMAND ----------

spark.readStream.table("orders_silver").createOrReplaceTempView("orders_silver_tmp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_silver_tmp

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view daily_customer_books_tmp
# MAGIC as
# MAGIC (select customer_id,firstname,lastname, date_trunc("DD",order_timestamp) order_date,sum(quantity) book_counts
# MAGIC from orders_silver_tmp
# MAGIC group by customer_id,firstname,lastname, date_trunc("DD",order_timestamp))

# COMMAND ----------

spark.table("daily_customer_books_tmp").writeStream.format("delta").option("checkpointLocation","dbfs:/mnt/demo/checkpoints/gold").outputMode("Complete").trigger(availableNow=True).table("daily_customer_books")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from daily_customer_books

# COMMAND ----------

load_new_data(all=True)

# COMMAND ----------

for s in spark.streams.active:
    print(f"Stopping stream {s}")
    s.stop()
    s.awaitTermination()

# COMMAND ----------


