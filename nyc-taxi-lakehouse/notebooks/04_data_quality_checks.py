# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Data quality checks
# MAGIC Each check is a SQL query that returns the number of violations (0 = pass). Results are appended to `ops.dq_results`; the notebook **fails the job** if any check fails.

# COMMAND ----------

from datetime import datetime

dbutils.widgets.text("catalog", "workspace", "Catalog")
catalog = dbutils.widgets.get("catalog")
B, S, G, O = (f"{catalog}.{x}" for x in ["bronze", "silver", "gold", "ops"])

# COMMAND ----------

checks = [
    ("silver", "trip_key_not_null",
     f"SELECT COUNT(*) FROM {S}.trips_clean WHERE trip_key IS NULL"),
    ("silver", "trip_key_unique",
     f"SELECT COUNT(*) - COUNT(DISTINCT trip_key) FROM {S}.trips_clean"),
    ("silver", "fare_positive",
     f"SELECT COUNT(*) FROM {S}.trips_clean WHERE fare_amount <= 0"),
    ("silver", "dropoff_after_pickup",
     f"SELECT COUNT(*) FROM {S}.trips_clean WHERE dropoff_ts < pickup_ts"),
    ("silver", "bronze_reconciles",
     f"""SELECT CASE WHEN (SELECT COUNT(*) FROM {B}.trips_raw)
                          - (SELECT COUNT(*) FROM {S}.trips_quarantine)
                          - (SELECT COUNT(*) FROM {S}.trips_clean) >= 0 THEN 0 ELSE 1 END"""),
    ("silver", "quarantine_rate_under_20pct",
     f"""SELECT CASE WHEN (SELECT COUNT(*) FROM {S}.trips_quarantine) * 1.0
                          / (SELECT COUNT(*) FROM {B}.trips_raw) > 0.20 THEN 1 ELSE 0 END"""),
    ("gold", "daily_kpis_reconcile_to_silver",
     f"SELECT ABS((SELECT SUM(trips) FROM {G}.daily_kpis) - (SELECT COUNT(*) FROM {S}.trips_clean))"),
    ("gold", "hourly_demand_reconciles_to_silver",
     f"SELECT ABS((SELECT SUM(trips) FROM {G}.hourly_demand) - (SELECT COUNT(*) FROM {S}.trips_clean))"),
]

run_ts = datetime.now()
rows = []
for layer, name, sql in checks:
    violations = int(spark.sql(sql).collect()[0][0])
    rows.append((run_ts, layer, name, violations, violations == 0))

result_df = spark.createDataFrame(
    rows, "run_ts timestamp, layer string, check_name string, violations long, passed boolean"
)
result_df.write.mode("append").saveAsTable(f"{O}.dq_results")
display(result_df)

# COMMAND ----------

failed = [r for r in rows if not r[4]]
if failed:
    raise Exception(f"{len(failed)} data quality check(s) failed: {[r[2] for r in failed]}")
print("All data quality checks passed.")
