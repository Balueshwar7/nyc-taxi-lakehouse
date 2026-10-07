# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: validate, quarantine, deduplicate, enrich
# MAGIC 1. Apply business rules in SQL and tag each bad row with a `reject_reason`
# MAGIC 2. Send bad rows to a **quarantine** table (nothing is silently dropped)
# MAGIC 3. Deduplicate using a deterministic `trip_key` + window function
# MAGIC 4. **MERGE** into the silver table so re-runs are idempotent

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
catalog = dbutils.widgets.get("catalog")
B, S, G, O = (f"{catalog}.{x}" for x in ["bronze", "silver", "gold", "ops"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Validation rules (CTE + CASE)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v_trips_validated AS
WITH enriched AS (
    SELECT
        tpep_pickup_datetime                                   AS pickup_ts,
        tpep_dropoff_datetime                                  AS dropoff_ts,
        CAST(trip_distance AS DOUBLE)                          AS distance_miles,
        CAST(fare_amount   AS DOUBLE)                          AS fare_amount,
        CAST(pickup_zip    AS STRING)                          AS pickup_zip,
        CAST(dropoff_zip   AS STRING)                          AS dropoff_zip,
        (unix_timestamp(tpep_dropoff_datetime)
           - unix_timestamp(tpep_pickup_datetime)) / 60.0      AS duration_min,
        _ingested_at
    FROM {B}.trips_raw
),
with_speed AS (
    SELECT *,
           distance_miles / NULLIF(duration_min, 0) * 60       AS avg_speed_mph
    FROM enriched
)
SELECT *,
    CASE
        WHEN pickup_ts IS NULL OR dropoff_ts IS NULL           THEN 'null_timestamp'
        WHEN pickup_zip IS NULL OR dropoff_zip IS NULL         THEN 'null_zip'
        WHEN dropoff_ts < pickup_ts                            THEN 'dropoff_before_pickup'
        WHEN duration_min < 1                                  THEN 'duration_too_short'
        WHEN duration_min > 180                                THEN 'duration_too_long'
        WHEN distance_miles <= 0                               THEN 'non_positive_distance'
        WHEN distance_miles > 100                              THEN 'distance_outlier'
        WHEN fare_amount <= 0                                  THEN 'non_positive_fare'
        WHEN fare_amount > 500                                 THEN 'fare_outlier'
        WHEN avg_speed_mph > 80                                THEN 'speed_outlier'
        ELSE NULL
    END AS reject_reason
FROM with_speed
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Quarantine table

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {S}.trips_quarantine
USING DELTA
COMMENT 'Rows that failed validation, with the reason'
AS SELECT * FROM v_trips_validated WHERE reject_reason IS NOT NULL
""")

display(spark.sql(f"""
SELECT reject_reason, COUNT(*) AS rows,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM {S}.trips_quarantine
GROUP BY reject_reason
ORDER BY rows DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Deduplicate + enrich (window function)

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW v_trips_stage AS
WITH clean AS (
    SELECT *,
        sha2(concat_ws('|',
            CAST(pickup_ts AS STRING), CAST(dropoff_ts AS STRING),
            pickup_zip, dropoff_zip,
            CAST(distance_miles AS STRING), CAST(fare_amount AS STRING)), 256) AS trip_key
    FROM v_trips_validated
    WHERE reject_reason IS NULL
),
dedup AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY trip_key ORDER BY _ingested_at DESC) AS rn
    FROM clean
)
SELECT
    trip_key,
    pickup_ts,
    dropoff_ts,
    CAST(pickup_ts AS DATE)                         AS pickup_date,
    hour(pickup_ts)                                 AS pickup_hour,
    dayofweek(pickup_ts)                            AS day_of_week_num,
    date_format(pickup_ts, 'EEEE')                  AS day_of_week,
    dayofweek(pickup_ts) IN (1, 7)                  AS is_weekend,
    CASE
        WHEN hour(pickup_ts) BETWEEN 7  AND 9  THEN 'morning_peak'
        WHEN hour(pickup_ts) BETWEEN 16 AND 19 THEN 'evening_peak'
        WHEN hour(pickup_ts) >= 22 OR hour(pickup_ts) <= 4 THEN 'night'
        ELSE 'off_peak'
    END                                             AS time_bucket,
    pickup_zip,
    dropoff_zip,
    distance_miles,
    fare_amount,
    ROUND(duration_min, 2)                          AS duration_min,
    ROUND(fare_amount / NULLIF(distance_miles, 0), 2) AS fare_per_mile,
    ROUND(avg_speed_mph, 1)                         AS avg_speed_mph,
    _ingested_at
FROM dedup
WHERE rn = 1
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Idempotent upsert with MERGE

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S}.trips_clean
USING DELTA
COMMENT 'Validated, deduplicated, enriched trips'
AS SELECT * FROM v_trips_stage WHERE 1 = 0
""")

spark.sql(f"""
MERGE INTO {S}.trips_clean AS t
USING v_trips_stage AS s
ON t.trip_key = s.trip_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

display(spark.sql(f"""
SELECT
    (SELECT COUNT(*) FROM {B}.trips_raw)        AS bronze_rows,
    (SELECT COUNT(*) FROM {S}.trips_quarantine) AS quarantined_rows,
    (SELECT COUNT(*) FROM {S}.trips_clean)      AS silver_rows
"""))
