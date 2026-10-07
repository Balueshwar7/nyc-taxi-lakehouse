# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: business-ready tables
# MAGIC Star-schema style tables built almost entirely with Spark SQL: CTEs, window functions (LAG, RANK, DENSE_RANK, NTILE, running sums), percentiles.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
catalog = dbutils.widgets.get("catalog")
B, S, G, O = (f"{catalog}.{x}" for x in ["bronze", "silver", "gold", "ops"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_date

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {G}.dim_date USING DELTA AS
WITH bounds AS (
    SELECT MIN(pickup_date) AS d0, MAX(pickup_date) AS d1 FROM {S}.trips_clean
)
SELECT
    d                                   AS date_key,
    year(d)                             AS year,
    quarter(d)                          AS quarter,
    month(d)                            AS month,
    date_format(d, 'MMMM')              AS month_name,
    weekofyear(d)                       AS week_of_year,
    dayofweek(d)                        AS day_of_week_num,
    date_format(d, 'EEEE')              AS day_name,
    dayofweek(d) IN (1, 7)              AS is_weekend
FROM (SELECT explode(sequence(d0, d1, interval 1 day)) AS d FROM bounds)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## daily_kpis - 7-day moving average, day-over-day change, running revenue

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {G}.daily_kpis USING DELTA AS
WITH daily AS (
    SELECT
        pickup_date,
        COUNT(*)                         AS trips,
        ROUND(SUM(fare_amount), 2)       AS revenue,
        ROUND(AVG(fare_amount), 2)       AS avg_fare,
        ROUND(AVG(distance_miles), 2)    AS avg_distance_miles,
        ROUND(AVG(duration_min), 1)      AS avg_duration_min
    FROM {S}.trips_clean
    GROUP BY pickup_date
)
SELECT
    *,
    ROUND(AVG(trips) OVER (ORDER BY pickup_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS trips_7d_avg,
    ROUND(100.0 * (trips - LAG(trips) OVER (ORDER BY pickup_date))
          / NULLIF(LAG(trips) OVER (ORDER BY pickup_date), 0), 1)                              AS trips_dod_pct,
    ROUND(SUM(revenue) OVER (ORDER BY pickup_date), 2)                                         AS cumulative_revenue
FROM daily
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## hourly_demand - ranking inside each weekday

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {G}.hourly_demand USING DELTA AS
WITH h AS (
    SELECT
        day_of_week, day_of_week_num, pickup_hour,
        COUNT(*)                        AS trips,
        ROUND(AVG(fare_amount), 2)      AS avg_fare,
        ROUND(AVG(fare_per_mile), 2)    AS avg_fare_per_mile
    FROM {S}.trips_clean
    GROUP BY day_of_week, day_of_week_num, pickup_hour
)
SELECT
    *,
    DENSE_RANK() OVER (PARTITION BY day_of_week ORDER BY trips DESC)       AS demand_rank_in_day,
    ROUND(100.0 * trips / SUM(trips) OVER (PARTITION BY day_of_week), 2)   AS pct_of_day
FROM h
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## top_routes - route ranking and share of total

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {G}.top_routes USING DELTA AS
WITH r AS (
    SELECT
        pickup_zip, dropoff_zip,
        COUNT(*)                        AS trips,
        ROUND(SUM(fare_amount), 2)      AS revenue,
        ROUND(AVG(fare_per_mile), 2)    AS avg_fare_per_mile,
        ROUND(AVG(duration_min), 1)     AS avg_duration_min
    FROM {S}.trips_clean
    GROUP BY pickup_zip, dropoff_zip
    HAVING COUNT(*) >= 5
)
SELECT
    *,
    RANK() OVER (ORDER BY trips DESC)                     AS route_rank,
    ROUND(100.0 * trips / SUM(trips) OVER (), 2)          AS pct_of_trips
FROM r
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## zip_performance - deciles and Pareto (cumulative revenue %)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {G}.zip_performance USING DELTA AS
WITH z AS (
    SELECT
        pickup_zip,
        COUNT(*)                                  AS trips,
        ROUND(SUM(fare_amount), 2)                AS revenue,
        ROUND(percentile_approx(duration_min, 0.5), 1) AS median_duration_min
    FROM {S}.trips_clean
    GROUP BY pickup_zip
)
SELECT
    *,
    NTILE(10) OVER (ORDER BY revenue DESC)        AS revenue_decile,
    ROUND(100.0 * SUM(revenue) OVER (ORDER BY revenue DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
          / SUM(revenue) OVER (), 2)              AS cumulative_revenue_pct
FROM z
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## trip_duration_profile - percentiles per time bucket

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {G}.trip_duration_profile USING DELTA AS
SELECT
    time_bucket,
    is_weekend,
    COUNT(*)                                                           AS trips,
    ROUND(AVG(duration_min), 1)                                        AS avg_duration_min,
    ROUND(percentile_approx(duration_min, 0.5), 1)                     AS p50_duration_min,
    ROUND(percentile_approx(duration_min, 0.9), 1)                     AS p90_duration_min,
    ROUND(percentile_approx(duration_min, 0.95), 1)                    AS p95_duration_min,
    ROUND(AVG(avg_speed_mph), 1)                                       AS avg_speed_mph
FROM {S}.trips_clean
GROUP BY time_bucket, is_weekend
""")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {G}.daily_kpis ORDER BY pickup_date"))
display(spark.sql(f"SELECT * FROM {G}.top_routes WHERE route_rank <= 10 ORDER BY route_rank"))
