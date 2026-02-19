# Databricks notebook source
CATALOG = "demo_mboto"
RAW_PATH = f"/Volumes/{CATALOG}/bronze/raw_orders"
CHECKPOINT = f"/Volumes/{CATALOG}/bronze/checkpoints/"
SCHEMA_LOCATION = f"/Volumes/{CATALOG}/bronze/checkpoints/bronze_schema"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql("USE SCHEMA bronze")

bronze_df = (
  spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .load(RAW_PATH)
    .drop("dt")
)

(bronze_df.writeStream
  .format("delta")
  .option("mergeSchema", "true")
  .option("checkpointLocation", CHECKPOINT)
  .trigger(availableNow=True) # batch like ingestion, recommended for optimizing compute costs.
  .toTable("bronze.orders_raw"))

# COMMAND ----------

