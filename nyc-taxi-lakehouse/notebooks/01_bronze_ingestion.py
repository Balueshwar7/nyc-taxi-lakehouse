# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: raw ingestion
# MAGIC Land the source data **as-is** into a Delta table and add lineage columns. No cleaning here - bronze is the replayable source of truth.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
catalog = dbutils.widgets.get("catalog")
B, S, G, O = (f"{catalog}.{x}" for x in ["bronze", "silver", "gold", "ops"])

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {B}.trips_raw
USING DELTA
COMMENT 'Raw NYC taxi trips, unmodified, plus ingestion metadata'
AS
SELECT
    *,
    current_timestamp()          AS _ingested_at,
    'samples.nyctaxi.trips'      AS _source
FROM samples.nyctaxi.trips
""")

# COMMAND ----------

display(spark.sql(f"SELECT COUNT(*) AS bronze_rows FROM {B}.trips_raw"))
display(spark.sql(f"DESCRIBE DETAIL {B}.trips_raw"))
