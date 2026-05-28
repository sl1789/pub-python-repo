"""Spark-parallel Monte Carlo simulation helpers.

Distributes simulation tasks across Spark workers using mapPartitions.
Each (ticker, K, T, method, num_runs) combo becomes an independent task
executed on a worker using the same pure-NumPy sim_* functions from
transforms.simulation.

When to use this vs simulation_helpers.py (driver-only):
─────────────────────────────────────────────────────────
USE SPARK (this module) when:
  - Multiple tickers (N > 3) or many (K, T) combos (> 20)
  - Cluster has workers (2+ nodes)
  - Total tasks > 100 (e.g. 10 tickers × 15 combos × 5 scales × 12 methods)
  - You want linear horizontal scaling (add nodes → proportional speedup)

USE DRIVER-ONLY (simulation_helpers.py) when:
  - Single ticker, few (K, T) combos
  - Single-node cluster (no workers — Spark adds overhead)
  - Quick ad-hoc runs where startup latency matters
  - Debugging (easier to step through sequential code)

Architecture:
─────────────
  Driver                              Workers
  ──────                              ───────
  1. Broadcast log_returns (200KB)    ← shared read-only
     + dist_params + sim_kwargs
  2. Create task RDD:
     [(ticker, K, T, method, runs, seed), ...]
  3. mapPartitions ─────────────────► Worker picks sim_* by name
                                      Runs NumPy simulation
                                      Computes payoffs, EMC, discount
                                      Returns (Row of results)
  4. Collect as Spark DataFrame  ◄──── Ready for Delta write
"""

from __future__ import annotations

import time
from typing import Iterator

import numpy as np
import pandas as pd

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import (
    StructType, StructField, IntegerType, FloatType, DoubleType, StringType,
)

from config.settings import get_logger

logger = get_logger(__name__)

# Same clip threshold as simulation_helpers.py
_MAX_LOG_SUM = 10.0

# Default cap for the analogue method (KD-tree loop is O(num_runs × T))
ANALOGUE_MAX_RUNS = 100_000

# Output schema matching existing Delta table
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


# ---------------------------------------------------------------------------
# Worker function (runs inside mapPartitions on each executor)
# ---------------------------------------------------------------------------

def _execute_tasks(
    task_iter: Iterator[Row],
    log_returns_bc,
    sim_kwargs_bc,
    emc_set_bc,
    risk_neutral_set_bc,
) -> Iterator[Row]:
    """Execute simulation tasks on a worker.

    Each task is a Row with (ticker, S0, K, T, method_name, num_runs, seed, r).
    The worker imports the sim_* function by name, runs it with NumPy,
    computes payoffs, applies EMC/discounting, and yields result Rows.
    """
    # Import on worker (avoids serialization issues with function objects)
    from transforms.simulation import SIMULATION_METHODS

    # Unpack broadcast variables (read once per partition, not per task)
    log_returns = log_returns_bc.value
    sim_kwargs = sim_kwargs_bc.value
    emc_set = emc_set_bc.value
    risk_neutral_set = risk_neutral_set_bc.value

    for task in task_iter:
        ticker = task["ticker"]
        S0 = task["S0"]
        K = task["K"]
        T = task["T"]
        method_name = task["method_name"]
        num_runs = task["num_runs"]
        seed = task["seed"]
        r = task["r"]

        # Look up simulation function by name
        sim_func = SIMULATION_METHODS.get(method_name)
        if sim_func is None:
            continue

        # Seed for reproducibility (unique per task)
        np.random.seed(seed)

        # Run simulation: returns (num_runs,) array of log-sums
        log_sums = sim_func(log_returns, num_runs, T, **sim_kwargs)

        # Clip extreme values
        log_sums = np.clip(log_sums, -_MAX_LOG_SUM, _MAX_LOG_SUM)

        # Terminal prices
        final_prices = S0 * np.exp(log_sums)

        # Empirical Martingale Correction
        if method_name in emc_set:
            sample_mean = float(np.mean(final_prices))
            if sample_mean > 0:
                target_mean = S0 * float(np.exp(r * T / 252.0))
                final_prices = final_prices * (target_mean / sample_mean)

        # Payoffs
        call_price = float(np.mean(np.maximum(final_prices - K, 0.0)))
        put_price = float(np.mean(np.maximum(K - final_prices, 0.0)))

        # Risk-neutral discounting
        if method_name in risk_neutral_set:
            discount = float(np.exp(-r * T / 252.0))
            call_price *= discount
            put_price *= discount

        yield Row(
            Runs=int(num_runs),
            K=float(np.float32(K)),  # match FloatType schema
            T=int(T),
            CallPrice=call_price,
            PutPrice=put_price,
            method=method_name,
            ticker=ticker,
            S0=S0,
        )


# ---------------------------------------------------------------------------
# Main entry point: Spark-parallel simulation
# ---------------------------------------------------------------------------

def run_simulations_spark(
    spark: SparkSession,
    log_returns: np.ndarray,
    S0: float,
    ticker: str,
    kt_pairs: list[tuple[float, int]],
    run_scales: list[int],
    dist_params: dict,
    alt_weight: float = 0.1,
    r: float = 0.0,
    lam: float = 0.4,
    methods: dict | None = None,
    extra_emc_methods: set[str] | frozenset[str] | None = None,
    analogue_max_runs: int = ANALOGUE_MAX_RUNS,
    n_partitions: int | None = None,
    base_seed: int = 42,
) -> DataFrame:
    """Run Monte Carlo simulations distributed across Spark workers.

    This is the Spark-parallel equivalent of calling run_simulations() in a
    loop over (K, T) pairs and run scales. Instead of sequential execution on
    the driver, it distributes all (K, T, method, scale) combos as independent
    tasks across the cluster.

    Args:
        spark: Active SparkSession.
        log_returns: 1D numpy array of historical log returns (~200KB).
        S0: Current underlying price.
        ticker: Ticker symbol (e.g. "SPY").
        kt_pairs: List of (strike, days_to_expiry) tuples.
        run_scales: List of num_runs values (e.g. [10_000, 100_000, 2_000_000]).
        dist_params: Distribution parameters from fit_distributions().
        alt_weight: Weight for Student-t mixing in sim_student_t.
        r: Annualized risk-free rate.
        lam: Intermittency parameter for multifractal methods.
        methods: Method registry dict. Defaults to SIMULATION_METHODS.
        extra_emc_methods: Additional methods to apply EMC correction.
        analogue_max_runs: Cap for the analogue method (skipped above this).
        n_partitions: Number of Spark partitions. Defaults to sc.defaultParallelism.
        base_seed: Base seed for reproducibility (each task gets a unique derived seed).

    Returns:
        Spark DataFrame with schema: Runs, K, T, CallPrice, PutPrice, method, ticker, S0
        (ready for Delta write via write_results).
    """
    from transforms.simulation import SIMULATION_METHODS, RISK_NEUTRAL_METHODS, EMC_METHODS

    if methods is None:
        methods = SIMULATION_METHODS

    emc_set = set(EMC_METHODS) | set(extra_emc_methods or [])
    risk_neutral_set = set(RISK_NEUTRAL_METHODS)

    vol = dist_params.get("vol", dist_params["std"] * np.sqrt(252))

    # Kwargs passed to all sim_* functions on workers
    sim_kwargs = {
        "alt_weight": alt_weight,
        "df": dist_params["df"],
        "tloc": dist_params["tloc"],
        "tscale": dist_params["tscale"],
        "r": r,
        "vol": vol,
        "lam": lam,
    }

    # --- Build task list ---
    tasks = []
    task_id = 0
    for K, T in kt_pairs:
        for num_runs in run_scales:
            for method_name in methods.keys():
                # Skip analogue at scales above the cap
                if method_name == "analogue" and num_runs > analogue_max_runs:
                    continue

                tasks.append(Row(
                    ticker=ticker,
                    S0=float(S0),
                    K=float(K),
                    T=int(T),
                    method_name=method_name,
                    num_runs=int(num_runs),
                    seed=int(base_seed + task_id),
                    r=float(r),
                ))
                task_id += 1

    n_tasks = len(tasks)
    if n_partitions is None:
        n_partitions = min(n_tasks, spark.sparkContext.defaultParallelism * 2)
    n_partitions = max(1, min(n_partitions, n_tasks))

    logger.info(
        f"[SPARK] Distributing {n_tasks} tasks across {n_partitions} partitions "
        f"({len(kt_pairs)} pairs × {len(run_scales)} scales × {len(methods)} methods)"
    )
    t0 = time.time()

    # --- Broadcast shared data ---
    sc = spark.sparkContext
    log_returns_bc = sc.broadcast(log_returns)
    sim_kwargs_bc = sc.broadcast(sim_kwargs)
    emc_set_bc = sc.broadcast(emc_set)
    risk_neutral_set_bc = sc.broadcast(risk_neutral_set)

    # --- Create task RDD and execute via mapPartitions ---
    task_rdd = sc.parallelize(tasks, numSlices=n_partitions)

    results_rdd = task_rdd.mapPartitions(
        lambda partition: _execute_tasks(
            partition,
            log_returns_bc,
            sim_kwargs_bc,
            emc_set_bc,
            risk_neutral_set_bc,
        )
    )

    # --- Convert to DataFrame ---
    results_df = spark.createDataFrame(results_rdd, schema=_RESULTS_SCHEMA)
    results_df = results_df.withColumn("created_at", current_timestamp())

    # Force materialization to get accurate timing
    count = results_df.cache().count()

    elapsed = time.time() - t0
    logger.info(f"[SPARK] Completed {count} results in {elapsed:.1f}s "
                f"({count/elapsed:.0f} tasks/s)")

    # --- Cleanup broadcasts ---
    log_returns_bc.unpersist()
    sim_kwargs_bc.unpersist()
    emc_set_bc.unpersist()
    risk_neutral_set_bc.unpersist()

    return results_df


# ---------------------------------------------------------------------------
# Multi-ticker convenience function
# ---------------------------------------------------------------------------

def run_multi_ticker_spark(
    spark: SparkSession,
    ticker_data: dict[str, dict],
    run_scales: list[int],
    alt_weight: float = 0.1,
    r: float = 0.0,
    lam: float = 0.4,
    methods: dict | None = None,
    extra_emc_methods: set[str] | frozenset[str] | None = None,
    analogue_max_runs: int = ANALOGUE_MAX_RUNS,
    n_partitions: int | None = None,
) -> DataFrame:
    """Run simulations for multiple tickers in a single Spark job.

    This avoids the overhead of creating separate Spark jobs per ticker.
    All tickers' tasks are pooled into one RDD for maximum parallelism.

    Args:
        spark: Active SparkSession.
        ticker_data: Dict mapping ticker -> {
            "log_returns": np.ndarray,
            "S0": float,
            "kt_pairs": [(K, T), ...],
            "dist_params": {...},
        }
        run_scales: List of num_runs values.
        alt_weight: Weight for Student-t mixing in sim_student_t.
        r: Annualized risk-free rate.
        lam: Intermittency parameter for multifractal methods.
        methods: Dict of {method_name: sim_func} to run. Defaults to all
            SIMULATION_METHODS. Pass a subset to limit which methods execute
            on workers (e.g. only EMC targets).
        extra_emc_methods: Additional methods to apply EMC correction.
        analogue_max_runs: Cap for analogue method.
        n_partitions: Number of Spark partitions.

    Returns:
        Spark DataFrame with all tickers' results combined.
    """
    from transforms.simulation import SIMULATION_METHODS, RISK_NEUTRAL_METHODS, EMC_METHODS

    if methods is None:
        methods = SIMULATION_METHODS

    # Only iterate over the requested method names
    method_names = list(methods.keys())

    emc_set = set(EMC_METHODS) | set(extra_emc_methods or [])
    risk_neutral_set = set(RISK_NEUTRAL_METHODS)

    sc = spark.sparkContext

    # Broadcast per-ticker data (each ticker's log_returns is ~200KB)
    ticker_returns_bc = sc.broadcast({
        t: data["log_returns"] for t, data in ticker_data.items()
    })

    # Build unified task list across all tickers
    all_tasks = []
    task_id = 0
    sim_kwargs_per_ticker = {}

    for ticker, data in ticker_data.items():
        dist_params = data["dist_params"]
        vol = dist_params.get("vol", dist_params["std"] * np.sqrt(252))
        sim_kwargs_per_ticker[ticker] = {
            "alt_weight": alt_weight,
            "df": dist_params["df"],
            "tloc": dist_params["tloc"],
            "tscale": dist_params["tscale"],
            "r": r,
            "vol": vol,
            "lam": lam,
        }

        for K, T in data["kt_pairs"]:
            for num_runs in run_scales:
                for method_name in method_names:
                    if method_name == "analogue" and num_runs > analogue_max_runs:
                        continue
                    all_tasks.append(Row(
                        ticker=ticker,
                        S0=float(data["S0"]),
                        K=float(K),
                        T=int(T),
                        method_name=method_name,
                        num_runs=int(num_runs),
                        seed=int(42 + task_id),
                        r=float(r),
                    ))
                    task_id += 1

    sim_kwargs_bc = sc.broadcast(sim_kwargs_per_ticker)
    emc_set_bc = sc.broadcast(emc_set)
    risk_neutral_set_bc = sc.broadcast(risk_neutral_set)

    n_tasks = len(all_tasks)
    if n_partitions is None:
        n_partitions = min(n_tasks, sc.defaultParallelism * 2)
    n_partitions = max(1, min(n_partitions, n_tasks))

    logger.info(
        f"[SPARK-MULTI] {len(ticker_data)} tickers, {n_tasks} total tasks "
        f"({len(method_names)} methods), {n_partitions} partitions"
    )
    t0 = time.time()

    task_rdd = sc.parallelize(all_tasks, numSlices=n_partitions)

    def _execute_multi(task_iter):
        """Worker function for multi-ticker execution."""
        from transforms.simulation import SIMULATION_METHODS

        returns_map = ticker_returns_bc.value
        kwargs_map = sim_kwargs_bc.value
        emc = emc_set_bc.value
        rn = risk_neutral_set_bc.value

        for task in task_iter:
            ticker = task["ticker"]
            S0 = task["S0"]
            K = task["K"]
            T = task["T"]
            method_name = task["method_name"]
            num_runs = task["num_runs"]
            seed = task["seed"]
            r_val = task["r"]

            sim_func = SIMULATION_METHODS.get(method_name)
            if sim_func is None:
                continue

            log_ret = returns_map[ticker]
            kwargs = kwargs_map[ticker]

            np.random.seed(seed)
            log_sums = sim_func(log_ret, num_runs, T, **kwargs)
            log_sums = np.clip(log_sums, -_MAX_LOG_SUM, _MAX_LOG_SUM)

            final_prices = S0 * np.exp(log_sums)

            if method_name in emc:
                sample_mean = float(np.mean(final_prices))
                if sample_mean > 0:
                    target_mean = S0 * float(np.exp(r_val * T / 252.0))
                    final_prices = final_prices * (target_mean / sample_mean)

            call_price = float(np.mean(np.maximum(final_prices - K, 0.0)))
            put_price = float(np.mean(np.maximum(K - final_prices, 0.0)))

            if method_name in rn:
                discount = float(np.exp(-r_val * T / 252.0))
                call_price *= discount
                put_price *= discount

            yield Row(
                Runs=int(num_runs),
                K=float(np.float32(K)),
                T=int(T),
                CallPrice=call_price,
                PutPrice=put_price,
                method=method_name,
                ticker=ticker,
                S0=S0,
            )

    results_rdd = task_rdd.mapPartitions(_execute_multi)
    results_df = spark.createDataFrame(results_rdd, schema=_RESULTS_SCHEMA)
    results_df = results_df.withColumn("created_at", current_timestamp())

    count = results_df.cache().count()
    elapsed = time.time() - t0
    logger.info(f"[SPARK-MULTI] Completed {count} results in {elapsed:.1f}s "
                f"({count/elapsed:.0f} tasks/s)")

    # Cleanup
    ticker_returns_bc.unpersist()
    sim_kwargs_bc.unpersist()
    emc_set_bc.unpersist()
    risk_neutral_set_bc.unpersist()

    return results_df
