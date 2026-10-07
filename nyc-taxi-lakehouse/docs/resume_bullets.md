# Resume bullets (fill the [brackets] with YOUR real numbers after running the project)

**Project title:** End-to-End Lakehouse Pipeline on Databricks (Medallion Architecture)
**Tech:** Databricks, PySpark, Spark SQL, Delta Lake, Unity Catalog, Databricks Workflows

- Built a bronze/silver/gold medallion pipeline on Databricks processing [X] NYC taxi trip records using PySpark with Spark SQL for all transformation logic.
- Implemented 10 rule-based validation checks with a quarantine table; isolated [X]% of records as invalid with a tracked `reject_reason` instead of dropping them.
- Designed idempotent loads using deterministic SHA-256 keys, ROW_NUMBER() de-duplication, and Delta `MERGE INTO`, so re-runs produce no duplicates.
- Created 5 gold tables (daily KPIs, hourly demand, top routes, ZIP Pareto, duration percentiles) using window functions (LAG, RANK, DENSE_RANK, NTILE, running aggregates).
- Added an automated data-quality framework (8 SQL checks logged to a Delta table) that fails the workflow on violations.
- Orchestrated the pipeline with Databricks Workflows (task dependencies, parameters, scheduled run) and used Delta time travel / OPTIMIZE for auditing and performance.

## Interview talking points
- Why medallion architecture? Replayability, separation of raw vs cleaned vs business data.
- Why quarantine instead of delete? Auditability and fixing upstream issues.
- Why MERGE with a deterministic key? Idempotency - safe to re-run after failures.
- What would you change at larger scale? Auto Loader / Structured Streaming for ingestion, partitioning or liquid clustering, Delta Live Tables / Lakeflow Declarative Pipelines.
