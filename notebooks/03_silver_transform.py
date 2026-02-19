# Databricks notebook source
from pyspark.sql import functions as sf

CATALOG = "demo_mboto"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql("USE SCHEMA silver")

bronze_table = f"{CATALOG}.bronze.orders_raw"
checkpoint = f"/Volumes/{CATALOG}/bronze/checkpoints/silver_orders/"

source = spark.readStream.table(bronze_table)

silver_df = (
    source
        .withColumn("order_ts", sf.to_timestamp("order_date")) # to_timestamp can take column or column name
        .drop("order_date")
        .withColumn("status", sf.upper("status"))
        .withColumn("amount", sf.col("amount").cast("double"))
        .dropDuplicates(["order_id"])
        .filter(sf.col("order_id").isNotNull() & sf.col("amount").isNotNull())
)

(silver_df.writeStream
 .format("delta")
 .option("mergeSchema", "true")
 .option("checkpointLocation", checkpoint)
 .trigger(availableNow=True)
 .toTable("silver.orders_clean"))