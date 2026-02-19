# Databricks notebook source
from pyspark.sql import functions as sf
from datetime import datetime, timedelta
import random, uuid

# Parameters
CATALOG = "demo_mboto"

RAW_BASE = f"/Volumes/{CATALOG}/bronze/raw_orders/"

# Data volume & time window
N_ORDERS = 10_000           # orders to generate for batch backfill
START_DATE = "2025-12-01"   # start of time window (inclusive)
END_DATE = "2026-01-31"     # end of time window (inclusive)

# Streaming trickle settings
TRICKLE_INTERVAL_SECS = 2   # seconds between events
TRICKLE_BURST_MIN, TRICKLE_BURST_MAX = 1, 5 # events per tick

# Set catalog context
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql("USE SCHEMA bronze")


# COMMAND ----------

from dateutil import parser

def random_datetime(start_iso, end_iso):
    start = parser.parse(start_iso)
    end = parser.parse(end_iso)
    delta = end - start
    rand_seconds = random.randint(0, int(delta.total_seconds()))
    return (start + timedelta(seconds=rand_seconds)).replace(microsecond=0).isoformat()

STATUSES = ["shipped", "processing", "canceled", "refunded"]
CHANNELS = ["web", "mobile", "in_store", "marketplace"]
COUNTRIES = ["PT", "ES", "FR", "DE", "UK", "US"]
CURRENCIES = {"PT": "EUR", "ES": "EUR", "FR": "EUR", "DE": "EUR", "UK": "GBP", "US": "USD"}

def make_order():
    country = random.choice(COUNTRIES)
    currency = CURRENCIES[country]
    amount = round(random.uniform(5, 500), 2)
    status = random.choices(STATUSES, weights=[0.72, 0.20, 0.06, 0.02], k=1)[0]
    channel = random.choices(CHANNELS, weights=[0.50, 0.25, 0.15, 0.10], k=1)[0]

    return {
        "order_id": f"O{uuid.uuid4().hex[:10].upper()}",
        "customer_id": random.randint(1, 5000),
        "order_date": random_datetime(START_DATE, END_DATE),
        "amount": amount,
        "status": status,
        "channel": channel,
        "country": country,
        "currency": currency
    }

# COMMAND ----------

# Generate an in-memory dataset
data = [make_order() for _ in range(N_ORDERS)]
df = spark.createDataFrame(data)

# This column is for partitioning the raw data.
df = df.withColumn("dt", sf.to_date(sf.col("order_date")))

# Write multiple small files for Auto Loader realism
n_files = 20
(df.repartition(n_files)
    .write
    .mode("append")
    .partitionBy("dt")
    .json(RAW_BASE))

print(f"Wrote ~{N_ORDERS} rows into {RAW_BASE}")



# COMMAND ----------

