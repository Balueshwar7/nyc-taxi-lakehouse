-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Sample business questions answered from the gold layer
-- MAGIC Replace `workspace` with your catalog name if different. Use these to build a Databricks SQL dashboard.

-- COMMAND ----------

-- Busiest 3 hours for each weekday
SELECT day_of_week, pickup_hour, trips, demand_rank_in_day
FROM workspace.gold.hourly_demand
WHERE demand_rank_in_day <= 3
ORDER BY day_of_week_num, demand_rank_in_day;

-- COMMAND ----------

-- Which pickup ZIPs make up the top 80% of revenue?
SELECT pickup_zip, revenue, cumulative_revenue_pct
FROM workspace.gold.zip_performance
WHERE cumulative_revenue_pct <= 80
ORDER BY revenue DESC;

-- COMMAND ----------

-- Days where trips dropped more than 20% vs the previous day
SELECT pickup_date, trips, trips_dod_pct
FROM workspace.gold.daily_kpis
WHERE trips_dod_pct < -20
ORDER BY pickup_date;

-- COMMAND ----------

-- Peak vs off-peak trip speed and duration
SELECT * FROM workspace.gold.trip_duration_profile ORDER BY time_bucket, is_weekend;
