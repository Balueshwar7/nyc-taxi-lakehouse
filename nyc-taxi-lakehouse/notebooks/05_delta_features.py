# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Delta Lake features (demo)
# MAGIC Table history, time travel, and file compaction - good talking points for interviews.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
catalog = dbutils.widgets.get("catalog")
B, S, G, O = (f"{catalog}.{x}" for x in ["bronze", "silver", "gold", "ops"])

# COMMAND ----------

# Audit trail of every write to the silver table
display(spark.sql(f"DESCRIBE HISTORY {S}.trips_clean"))

# COMMAND ----------

# Time travel: compare the first version to the current one
display(spark.sql(f"""
SELECT
    (SELECT COUNT(*) FROM {S}.trips_clean VERSION AS OF 0) AS rows_v0,
    (SELECT COUNT(*) FROM {S}.trips_clean)                 AS rows_now
"""))

# COMMAND ----------

# Compact small files
spark.sql(f"OPTIMIZE {S}.trips_clean")
display(spark.sql(f"DESCRIBE DETAIL {S}.trips_clean"))
