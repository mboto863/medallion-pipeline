# Databricks notebook source
from pyspark.sql import functions as sf

CATALOG = "demo_mboto"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql("USE SCHEMA gold")

silver_table = f"{CATALOG}.silver.orders_clean"
checkpoint = f"/Volumes/{CATALOG}/bronze/checkpoints/gold_orders/"

source = spark.readStream.table(silver_table)

gold_df = (
    source
        .withColumn("order_date", sf.to_date("order_ts"))
        .groupBy("order_date", "status")
        .agg(
            sf.sum("amount").alias("total_revenue"),
            sf.count("*").alias("order_count"),
            sf.approx_count_distinct("customer_id").alias("unique_customers")
        )
)

(gold_df.writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .toTable("gold.revenue_daily"))