"""Simulation pipeline helper functions.

Orchestration utilities for building broadcast variables, creating
target DataFrames, running all simulation methods, aggregating
results, and writing output.
"""

from __future__ import annotations

from itertools import product

import numpy as np
import pandas as pd
import scipy.stats
from scipy.stats import t as t_dist

from pyspark.sql import DataFrame, SparkSession, functions as F, Window
from pyspark.sql.functions import lit, expr, avg, current_timestamp
from pyspark.sql.types import (
    FloatType, IntegerType, StructType, StructField,
)

from config.settings import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Data loading and distribution fitting
# ---------------------------------------------------------------------------

def load_historical_data(
    spark: SparkSession, table_name: str, ticker: str
) -> tuple[DataFrame, float]:
    """Load historical data for a ticker and return (DataFrame, S0).

    Args:
        spark: Active SparkSession.
        table_name: Fully qualified table name.
        ticker: Stock ticker symbol.

    Returns:
        Tuple of (Spark DataFrame, latest adjusted close price S0).
    """
    historical_df = spark.sql(f"""
        SELECT * FROM {table_name}
        WHERE ticker = '{ticker}'
        ORDER BY Date
    """)

    row_count = historical_df.count()
    logger.info(f"Loaded {row_count} rows for ticker '{ticker}'")

    if row_count == 0:
        raise ValueError(f"No data found for ticker '{ticker}' in {table_name}")

    S0 = float(
        historical_df.orderBy(F.col("Date").desc()).select("Adj_Close").first()[0]
    )
    logger.info(f"S0 (latest price): {S0:.2f}")
    return historical_df, S0


def fit_distributions(historical_df: DataFrame) -> dict:
    """Fit Cauchy and Student-t distributions to historical log returns.

    Returns:
        Dict with keys: mu, std, loc, scale, deg_f, tloc, tscale, log_array.
    """
    pd_hist = historical_df.select("log_return").toPandas()
    log_array = pd_hist[["log_return"]].to_numpy()

    mu = float(np.mean(log_array))
    std = float(np.std(log_array))

    params_cauchy = scipy.stats.cauchy.fit(log_array)
    loc = float(params_cauchy[-2])
    scale = float(params_cauchy[-1])

    params_t = t_dist.fit(log_array)
    deg_f = float(params_t[0])
    tloc = float(params_t[-2])
    tscale = float(params_t[-1])

    logger.info(f"mu={mu:.6f}, std={std:.6f}")
    logger.info(f"Cauchy: loc={loc:.6f}, scale={scale:.6f}")
    logger.info(f"Student-t: df={deg_f:.4f}, loc={tloc:.6f}, scale={tscale:.6f}")

    return {
        "mu": mu, "std": std,
        "loc": loc, "scale": scale,
        "deg_f": deg_f, "tloc": tloc, "tscale": tscale,
        "log_array": log_array,
    }


# ---------------------------------------------------------------------------
# Broadcast variables
# ---------------------------------------------------------------------------

def build_and_broadcast(
    spark: SparkSession, historical_df: DataFrame, dist_params: dict
) -> dict:
    """Build the log-return dictionary and broadcast all variables.

    Returns:
        Dict of broadcast variable references (b, b_loc, b_scale, etc.).
    """
    df_index = historical_df.select("pct_change", "log_return").withColumn(
        "id", F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
    )

    new_dict = df_index.select("id", "log_return").toPandas()
    dict_ret = new_dict.set_index("id")["log_return"].dropna().to_dict()

    logger.info(f"Dictionary size: {len(dict_ret)} entries")

    sc = spark.sparkContext
    broadcasts = {
        "b": sc.broadcast(dict_ret),
        "b_loc": sc.broadcast(dist_params["loc"]),
        "b_scale": sc.broadcast(dist_params["scale"]),
        "b_deg_f": sc.broadcast(dist_params["deg_f"]),
        "b_tloc": sc.broadcast(dist_params["tloc"]),
        "b_tscale": sc.broadcast(dist_params["tscale"]),
        "b_mu": sc.broadcast(dist_params["mu"]),
        "b_std": sc.broadcast(dist_params["std"]),
    }

    logger.info("All variables broadcast to workers.")
    return broadcasts


# ---------------------------------------------------------------------------
# Target DataFrame
# ---------------------------------------------------------------------------

def create_target_dataframe(
    spark: SparkSession, K: float, T: int, num_runs: int
) -> DataFrame:
    """Create a target DataFrame with one row per simulation run."""
    result = list(product([K], [T], [num_runs]))
    pdf = pd.DataFrame(result, columns=["K", "T", "Runs"])

    schema = StructType([
        StructField("K", FloatType(), False),
        StructField("T", IntegerType(), False),
        StructField("Runs", IntegerType(), False),
    ])
    df_target = spark.createDataFrame(pdf, schema)

    l_ind = [[num_runs, j] for j in range(1, num_runs + 1)]
    dfRuns = pd.DataFrame(l_ind, columns=["Runs", "No"])
    schemaRuns = StructType([
        StructField("Runs", IntegerType(), False),
        StructField("No", IntegerType(), False),
    ])
    dfR = spark.createDataFrame(dfRuns, schemaRuns)

    target_df = df_target.join(dfR, on="Runs", how="inner")
    logger.info(f"Target DataFrame: {target_df.count()} rows (K={K}, T={T}, Runs={num_runs})")
    return target_df


# ---------------------------------------------------------------------------
# Run simulations
# ---------------------------------------------------------------------------

def run_simulation(target_df: DataFrame, udf_fn, S0: float) -> DataFrame:
    """Apply a single simulation UDF and compute option payoffs."""
    return (
        target_df.select("*")
        .withColumn("logsum", udf_fn()(F.col("T")))
        .withColumn("sum", lit(S0) * F.exp("logsum"))
        .withColumn("priceCall", expr("IF(sum > K, sum - K, 0.0)"))
        .withColumn("pricePut", expr("IF(sum < K, K - sum, 0.0)"))
    )


def run_all_simulations(
    target_df: DataFrame, S0: float, udf_map: dict
) -> dict[str, DataFrame]:
    """Run all simulation methods and return {method_name: result_df}.

    Args:
        target_df: Target DataFrame with K, T, Runs, No columns.
        S0: Latest adjusted close price.
        udf_map: Dict mapping method_name -> udf factory function.

    Returns:
        Dict of {method_name: DataFrame with simulation results}.
    """
    results = {}
    for i, (name, udf_fn) in enumerate(udf_map.items(), 1):
        results[name] = run_simulation(target_df, udf_fn, S0)
        logger.info(f"  [{i}/{len(udf_map)}] {name} - defined")
    return results


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def aggregate_results(
    simulation_results: dict[str, DataFrame], ticker: str, S0: float
) -> DataFrame:
    """Aggregate all simulation results and add metadata columns."""
    agg_dfs = []
    for method_name, df in simulation_results.items():
        agg = (
            df.groupBy("Runs", "K", "T")
            .agg(
                avg("priceCall").alias("CallPrice"),
                avg("pricePut").alias("PutPrice"),
            )
            .withColumn("method", lit(method_name))
        )
        agg_dfs.append(agg)

    # Union all method results
    combined = agg_dfs[0]
    for agg in agg_dfs[1:]:
        combined = combined.unionByName(agg)

    # Add metadata
    combined = (
        combined
        .withColumn("ticker", lit(ticker))
        .withColumn("S0", lit(S0))
        .withColumn("created_at", current_timestamp())
    )

    logger.info(f"Aggregated results: {combined.count()} rows ({len(simulation_results)} methods)")
    return combined


# ---------------------------------------------------------------------------
# Write results
# ---------------------------------------------------------------------------

def write_results(
    agg_df: DataFrame,
    table_name: str,
    ticker: str,
    export_path: str | None = None,
) -> None:
    """Write aggregated results to Delta table and optionally export to Parquet."""
    spark = agg_df.sparkSession

    # Ensure database exists
    db = table_name.split(".")[0]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

    # Write to Delta (append mode)
    (
        agg_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("ticker")
        .saveAsTable(table_name)
    )
    logger.info(f"[OK] delta_table={table_name} (partition ticker={ticker})")

    # Optional Parquet export
    if export_path:
        try:
            (
                agg_df.drop("created_at")
                .write
                .mode("overwrite")
                .parquet(export_path)
            )
            logger.info(f"[OK] parquet_export={export_path}")
        except Exception as e:
            logger.warning(f"Parquet export failed (storage may not be configured): {e}")
            logger.info("Delta table write was successful - results are persisted.")
