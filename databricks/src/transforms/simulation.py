"""Monte Carlo simulation functions for option pricing.

Contains random generators, simulation methods (7 variants), and
Spark UDF factories. Call `init_broadcast()` before using UDFs to
set the broadcast variable references used by simulation functions.
"""

from __future__ import annotations

import os
import binascii

import numpy as np
import scipy.stats
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

# ---------------------------------------------------------------------------
# Module-level broadcast variable references (set via init_broadcast)
# ---------------------------------------------------------------------------
b = None          # broadcast dict: {id -> log_return}
b_loc = None      # Cauchy loc
b_scale = None    # Cauchy scale
b_deg_f = None    # Student-t degrees of freedom
b_tloc = None     # Student-t loc
b_tscale = None   # Student-t scale
b_mu = None       # Mean log return
b_std = None      # Std log return


def init_broadcast(
    broadcast_dict,
    broadcast_loc,
    broadcast_scale,
    broadcast_deg_f,
    broadcast_tloc,
    broadcast_tscale,
    broadcast_mu,
    broadcast_std,
) -> None:
    """Initialize module-level broadcast variable references.

    Must be called once after broadcasting variables from the driver.
    """
    global b, b_loc, b_scale, b_deg_f, b_tloc, b_tscale, b_mu, b_std
    b = broadcast_dict
    b_loc = broadcast_loc
    b_scale = broadcast_scale
    b_deg_f = broadcast_deg_f
    b_tloc = broadcast_tloc
    b_tscale = broadcast_tscale
    b_mu = broadcast_mu
    b_std = broadcast_std


# ---------------------------------------------------------------------------
# Random generators
# ---------------------------------------------------------------------------

def GetRandomIndex(x: int, seed: int | None = None) -> int:
    """Generate a random integer in [0, x)."""
    seed = seed if seed is not None else int(binascii.hexlify(os.urandom(4)), 16)
    rs = np.random.RandomState(seed)
    return rs.randint(x)


def GetExpon(y: float = 1) -> float:
    """Draw from an exponential distribution with scale y."""
    return np.random.exponential(scale=y, size=None)


def FlipCoin(x=(-1, 1)) -> float:
    """Random choice between x values with equal probability."""
    return np.random.choice(x, None, [0.5, 0.5])


def FlipExpon(y: float = 0.1, x=(-1, 1)) -> float:
    """Exponential draw multiplied by a random sign."""
    return GetExpon(y) * FlipCoin(x)


def GetRandomFlip(list1, ch=(0.9, 0.1), seed: int | None = None):
    """Weighted random choice from list1."""
    seed = seed if seed is not None else int(binascii.hexlify(os.urandom(4)), 16)
    rs = np.random.RandomState(seed)
    return rs.choice(list1, None, p=ch)


def _get_dict(a: int):
    """Lookup value from broadcast dictionary."""
    return b.value.get(a)


# ---------------------------------------------------------------------------
# Simulation methods
# ---------------------------------------------------------------------------

def f_historical(y: int) -> float:
    """Historical dictionary sampling: sum of y random log returns."""
    lsum = 0.0
    for _ in range(y):
        a = GetRandomIndex(len(b.value))
        v = _get_dict(a)
        lsum += v if v is not None else 0.0
    return lsum


def f_expon(y: int, ch=(0.9, 0.1)) -> float:
    """90% historical + 10% exponential flip."""
    lsum = 0.0
    for _ in range(y):
        coin = GetRandomFlip(["hist", "expon"], ch)
        if coin == "hist":
            a = GetRandomIndex(len(b.value))
            v = _get_dict(a)
            lsum += v if v is not None else 0.0
        else:
            lsum += FlipExpon()
    return lsum


def f_cauchy(y: int, ch=(0.9, 0.1)) -> float:
    """90% historical + 10% Cauchy draw."""
    lsum = 0.0
    for _ in range(y):
        coin = GetRandomFlip(["hist", "cauchy"], ch)
        if coin == "hist":
            a = GetRandomIndex(len(b.value))
            v = _get_dict(a)
            lsum += v if v is not None else 0.0
        else:
            lsum += scipy.stats.cauchy.rvs(b_loc.value, b_scale.value)
    return lsum


def f_t(y: int, ch=(0.9, 0.1)) -> float:
    """90% historical + 10% Student-t draw."""
    lsum = 0.0
    for _ in range(y):
        coin = GetRandomFlip(["hist", "t"], ch)
        if coin == "hist":
            a = GetRandomIndex(len(b.value))
            v = _get_dict(a)
            lsum += v if v is not None else 0.0
        else:
            lsum += scipy.stats.t.rvs(b_deg_f.value, loc=b_tloc.value, scale=b_tscale.value)
    return lsum


def f_window(y: int) -> float:
    """Consecutive window of y days from a random start."""
    lsum = 0.0
    a = GetRandomIndex(len(b.value))
    length = len(b.value)
    for i in range(y):
        v = _get_dict((a + i) % length)
        lsum += v if v is not None else 0.0
    return lsum


def f_tenwindow(y: int) -> float:
    """Window with 10-day step."""
    lsum = 0.0
    a = GetRandomIndex(len(b.value))
    length = len(b.value)
    for i in range(y):
        v = _get_dict((a + 10 * i) % length)
        lsum += v if v is not None else 0.0
    return lsum


def f_twentywindow(y: int) -> float:
    """Window with 20-day step."""
    lsum = 0.0
    a = GetRandomIndex(len(b.value))
    length = len(b.value)
    for i in range(y):
        v = _get_dict((a + 20 * i) % length)
        lsum += v if v is not None else 0.0
    return lsum


# ---------------------------------------------------------------------------
# UDF factories
# ---------------------------------------------------------------------------

def udf_historical():
    """UDF: historical dictionary simulation."""
    return udf(lambda c: f_historical(c), FloatType())


def udf_expon():
    """UDF: 90% historical + 10% exponential."""
    return udf(lambda c: f_expon(c, ch=(0.9, 0.1)), FloatType())


def udf_cauchy():
    """UDF: 90% historical + 10% Cauchy."""
    return udf(lambda c: f_cauchy(c, ch=(0.9, 0.1)), FloatType())


def udf_t():
    """UDF: 90% historical + 10% Student-t."""
    return udf(lambda c: f_t(c, ch=(0.9, 0.1)), FloatType())


def udf_window():
    """UDF: consecutive window."""
    return udf(lambda c: f_window(c), FloatType())


def udf_ten_window():
    """UDF: 10-day step window."""
    return udf(lambda c: f_tenwindow(c), FloatType())


def udf_twenty_window():
    """UDF: 20-day step window."""
    return udf(lambda c: f_twentywindow(c), FloatType())
