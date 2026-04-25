# Databricks notebook: jobs/simple_job_results
# Purpose:
# - Read job parameters (job_id, start_date, end_date)
# - Generate a tiny toy dataset
# - Write results to a Delta table (authoritative)
# - Export results to Parquet snapshot in ADLS:
# abfss://results@<storage_account>.dfs.core.windows.net/export/job_id=<job_id>/
from datetime import date, timedelta
from pyspark.sql import functions as F
from pyspark.sql import types as T

# -------------------------
# 1) Widgets (safe defaults for interactive runs)
# -------------------------
# If the notebook runs as a Job, the widgets should be populated fromnotebook_params.
# For interactive debugging, these defaults allow manual execution.
dbutils.widgets.text("job_id", "0", "job_id")
dbutils.widgets.text("start_date", "2026-01-01", "start_date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "2026-01-03", "end_date (YYYY-MM-DD)")

job_id = int(dbutils.widgets.get("job_id"))
start_date = date.fromisoformat(dbutils.widgets.get("start_date"))
end_date = date.fromisoformat(dbutils.widgets.get("end_date"))

# -------------------------
# 2) Define schema (explicit)
# -------------------------
schema = T.StructType([
    T.StructField("job_id", T.IntegerType(), False),
    T.StructField("business_date", T.DateType(), False),
    T.StructField("metric_name", T.StringType(), False),
    T.StructField("metric_value", T.DoubleType(), False),
    T.StructField("created_at", T.TimestampType(), False),
])
# -------------------------
# 3) Generate toy data
# -------------------------

rows = []
d = start_date
while d <= end_date:
    rows.append((job_id, d, "toy_metric", float(100.0), None))
    d += timedelta(days=1)
    df = spark.createDataFrame(rows, schema=schema).withColumn("created_at",F.current_timestamp())
    
# -------------------------
# 4) Write to Delta table (authoritative)
# -------------------------
# One table for all jobs, partitioned by job_id.
# Idempotent: overwrite the partition for this job_id.
spark.sql("CREATE DATABASE IF NOT EXISTS demo")

# Use a managed table in the metastore (simple for a toy project).
# If you prefer a specific location, use .save(<path>) and then CREATE TABLE USING DELTA LOCATION.

(
df.write
.format("delta")
.mode("overwrite")
.option("overwriteSchema", "true")
.partitionBy("job_id")
.saveAsTable("demo.job_results")
)
# -------------------------
# 5) Export Parquet snapshot for the API/UI
# -------------------------
# This must match your API reader path pattern exactly.
# NOTE: Replace <storage-account> with your actual storage account name.

storage_account = "esgteamstorage"
container = "results"
prefix = "export"

export_path =f"abfss://{container}@{storage_account}.dfs.core.windows.net/{prefix}/job_id={job_id}"

(
df.drop("created_at") # optional: keep export smaller; keep it if you want
.write
.mode("overwrite")
.parquet(export_path)
)

print(f"[OK] job_id={job_id}")
print(f"[OK] delta_table=demo.job_results (partition job_id={job_id})")
print(f"[OK] parquet_export={export_path}")