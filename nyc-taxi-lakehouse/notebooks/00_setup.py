# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup
# MAGIC Creates the medallion schemas (bronze / silver / gold) and the ops table that stores data-quality results.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
catalog = dbutils.widgets.get("catalog")
B, S, G, O = (f"{catalog}.{x}" for x in ["bronze", "silver", "gold", "ops"])

# COMMAND ----------

for sch in ["bronze", "silver", "gold", "ops"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{sch}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {O}.dq_results (
    run_ts     TIMESTAMP,
    layer      STRING,
    check_name STRING,
    violations BIGINT,
    passed     BOOLEAN
) USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Quick look at the source data (built into every Databricks workspace).

# COMMAND ----------

display(spark.sql("SELECT * FROM samples.nyctaxi.trips LIMIT 10"))
display(spark.sql("SELECT COUNT(*) AS rows, MIN(tpep_pickup_datetime) AS first_trip, MAX(tpep_pickup_datetime) AS last_trip FROM samples.nyctaxi.trips"))
