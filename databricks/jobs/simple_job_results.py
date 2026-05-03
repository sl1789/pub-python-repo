# Databricks notebook entrypoint: jobs/simple_job_results
#
# All real logic lives in `databricks.lib.*` so it can be unit-tested
# without a Spark cluster. This file only:
#   1) declares widgets (with safe interactive defaults), and
#   2) delegates to `run_and_exit`.
#
# Required widgets (passed by the API's DatabricksRunner via job_parameters):
#   job_id, start_date, end_date, storage_account, container, prefix,
#   database, table

# When running on a Databricks cluster the repo root must be on sys.path so
# `databricks.lib` is importable. Configure this with %pip / repo settings
# in production; the snippet below makes interactive runs work too.
import os
import sys

_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from databricks.lib.entrypoint import run_and_exit  # noqa: E402

# Interactive defaults; Job runs supply these via notebook_params.
dbutils.widgets.text("job_id", "1", "job_id")
dbutils.widgets.text("start_date", "2026-01-01", "start_date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "2026-01-03", "end_date (YYYY-MM-DD)")
dbutils.widgets.text("storage_account", "", "storage_account")
dbutils.widgets.text("container", "results", "container")
dbutils.widgets.text("prefix", "export", "prefix")
dbutils.widgets.text("database", "demo", "database")
dbutils.widgets.text("table", "job_results", "table")

run_and_exit(spark, dbutils)
