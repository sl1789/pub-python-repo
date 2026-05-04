"""Simulation pipeline helper functions.

Orchestration utilities for loading data, fitting distributions,
running NumPy-vectorized Monte Carlo simulations on the driver,
and writing results to Delta tables.
"""

from __future__ import annotations

import time

import numpy as np
import pandas as pd
import scipy.stats
from scipy.stats import t as t_dist

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, IntegerType, FloatType, DoubleType, StringType,
)

from config.settings import get_logger
from transforms.simulation import SIMULATION_METHODS

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Data loading and distribution fitting
# ---------------------------------------------------------------------------

def load_historical_data(
    spark: SparkSession, table_name: str, ticker: str
) -> tuple[np.ndarray, float]:
    """Load historical data for a ticker and return (log_returns array, S0).

    Returns:
        Tuple of (log_returns as 1D numpy array, latest adjusted close price).
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

    # Extract log returns as a flat numpy array
    pd_hist = historical_df.select("log_return").toPandas()
    log_returns = pd_hist["log_return"].dropna().to_numpy().astype(np.float64)
    logger.info(f"Log returns array: {len(log_returns)} values")

    return log_returns, S0


def fit_distributions(log_returns: np.ndarray) -> dict:
    """Fit Student-t distribution to log returns."""
    mu = float(np.mean(log_returns))
    std = float(np.std(log_returns))

    params_t = t_dist.fit(log_returns)
    deg_f = float(params_t[0])
    tloc = float(params_t[-2])
    tscale = float(params_t[-1])

    logger.info(f"mu={mu:.6f}, std={std:.6f}")
    logger.info(f"Student-t: df={deg_f:.4f}, loc={tloc:.6f}, scale={tscale:.6f}")

    return {
        "mu": mu, "std": std,
        "df": deg_f, "tloc": tloc, "tscale": tscale,
    }


# ---------------------------------------------------------------------------
# Run simulations (NumPy on driver)
# ---------------------------------------------------------------------------

# Clip log-sums to prevent extreme outliers from dominating the mean.
# exp(10) ≈ 22,026x which allows extreme stress scenarios but prevents
# a single outlier from producing inf or dominating 100K paths.
_MAX_LOG_SUM = 10.0


def run_simulations(
    log_returns: np.ndarray,
    S0: float,
    K: float,
    T: int,
    num_runs: int,
    dist_params: dict,
    alt_weight: float = 0.1,
    methods: dict | None = None,
) -> pd.DataFrame:
    """Run all simulation methods using NumPy and return results as pandas DataFrame.

    Args:
        log_returns: 1D array of historical log returns.
        S0: Latest adjusted close price.
        K: Strike price.
        T: Simulation period in days.
        num_runs: Number of Monte Carlo paths.
        dist_params: Distribution parameters from fit_distributions().
        alt_weight: Weight for alternative distributions.
        methods: Dict of method_name -> function. Defaults to SIMULATION_METHODS.

    Returns:
        pandas DataFrame with columns: Runs, K, T, CallPrice, PutPrice, method.
    """
    if methods is None:
        methods = SIMULATION_METHODS

    # Common kwargs passed to all simulation functions
    sim_kwargs = {
        "alt_weight": alt_weight,
        "df": dist_params["df"],
        "tloc": dist_params["tloc"],
        "tscale": dist_params["tscale"],
    }

    results = []
    for method_name, sim_func in methods.items():
        t_start = time.time()

        # Run simulation: returns array of log-sums (one per path)
        log_sums = sim_func(log_returns, num_runs, T, **sim_kwargs)

        # Clip extreme log-sums to prevent overflow / outlier dominance
        log_sums = np.clip(log_sums, -_MAX_LOG_SUM, _MAX_LOG_SUM)

        # Compute final prices and option payoffs
        final_prices = S0 * np.exp(log_sums)
        call_payoffs = np.maximum(final_prices - K, 0.0)
        put_payoffs = np.maximum(K - final_prices, 0.0)

        call_price = float(np.mean(call_payoffs))
        put_price = float(np.mean(put_payoffs))

        elapsed = time.time() - t_start
        logger.info(f"  {method_name}: Call=${call_price:.4f}, Put=${put_price:.4f} ({elapsed:.3f}s)")

        results.append({
            "Runs": num_runs,
            "K": K,
            "T": T,
            "CallPrice": call_price,
            "PutPrice": put_price,
            "method": method_name,
        })

    return pd.DataFrame(results)


# Schema matching the existing Delta table (IntegerType for Runs/T, FloatType for K)
_RESULTS_SCHEMA = StructType([
    StructField("Runs", IntegerType(), False),
    StructField("K", FloatType(), False),
    StructField("T", IntegerType(), False),
    StructField("CallPrice", DoubleType(), True),
    StructField("PutPrice", DoubleType(), True),
    StructField("method", StringType(), False),
    StructField("ticker", StringType(), False),
    StructField("S0", DoubleType(), False),
])


def results_to_spark(
    spark: SparkSession,
    results_pdf: pd.DataFrame,
    ticker: str,
    S0: float,
) -> DataFrame:
    """Convert pandas results to Spark DataFrame with metadata columns."""
    results_pdf = results_pdf.copy()
    results_pdf["ticker"] = ticker
    results_pdf["S0"] = S0

    # Cast types to match existing Delta table schema
    results_pdf["Runs"] = results_pdf["Runs"].astype("int32")
    results_pdf["T"] = results_pdf["T"].astype("int32")
    results_pdf["K"] = results_pdf["K"].astype("float32")

    df = spark.createDataFrame(results_pdf, schema=_RESULTS_SCHEMA)
    df = df.withColumn("created_at", current_timestamp())

    logger.info(f"Results DataFrame: {df.count()} rows ({len(results_pdf)} methods)")
    return df


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
