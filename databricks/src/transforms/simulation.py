"""Monte Carlo simulation functions for option pricing.

Contains random generators, simulation methods (7 variants), and
Spark UDF factories. UDFs use closures to capture broadcast variables,
ensuring they are correctly serialized to Spark workers.
"""

from __future__ import annotations

import os
import binascii

import numpy as np
import scipy.stats
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType


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


# ---------------------------------------------------------------------------
# UDF factories (closure-based — captures broadcast vars for workers)
# ---------------------------------------------------------------------------

def udf_historical(b_dict):
    """UDF factory: historical dictionary simulation."""
    def _sim(y):
        lsum = 0.0
        d = b_dict.value
        n = len(d)
        for _ in range(y):
            a = GetRandomIndex(n)
            v = d.get(a)
            lsum += v if v is not None else 0.0
        return lsum
    return udf(lambda c: _sim(c), FloatType())


def udf_expon(b_dict):
    """UDF factory: 90% historical + 10% exponential flip."""
    def _sim(y):
        lsum = 0.0
        d = b_dict.value
        n = len(d)
        for _ in range(y):
            coin = GetRandomFlip(["hist", "expon"], (0.9, 0.1))
            if coin == "hist":
                a = GetRandomIndex(n)
                v = d.get(a)
                lsum += v if v is not None else 0.0
            else:
                lsum += FlipExpon()
        return lsum
    return udf(lambda c: _sim(c), FloatType())


def udf_cauchy(b_dict, b_loc, b_scale):
    """UDF factory: 90% historical + 10% Cauchy draw."""
    def _sim(y):
        lsum = 0.0
        d = b_dict.value
        n = len(d)
        loc_val = b_loc.value
        scale_val = b_scale.value
        for _ in range(y):
            coin = GetRandomFlip(["hist", "cauchy"], (0.9, 0.1))
            if coin == "hist":
                a = GetRandomIndex(n)
                v = d.get(a)
                lsum += v if v is not None else 0.0
            else:
                lsum += scipy.stats.cauchy.rvs(loc_val, scale_val)
        return lsum
    return udf(lambda c: _sim(c), FloatType())


def udf_t(b_dict, b_deg_f, b_tloc, b_tscale):
    """UDF factory: 90% historical + 10% Student-t draw."""
    def _sim(y):
        lsum = 0.0
        d = b_dict.value
        n = len(d)
        df_val = b_deg_f.value
        tloc_val = b_tloc.value
        tscale_val = b_tscale.value
        for _ in range(y):
            coin = GetRandomFlip(["hist", "t"], (0.9, 0.1))
            if coin == "hist":
                a = GetRandomIndex(n)
                v = d.get(a)
                lsum += v if v is not None else 0.0
            else:
                lsum += scipy.stats.t.rvs(df_val, loc=tloc_val, scale=tscale_val)
        return lsum
    return udf(lambda c: _sim(c), FloatType())


def udf_window(b_dict):
    """UDF factory: consecutive window."""
    def _sim(y):
        lsum = 0.0
        d = b_dict.value
        n = len(d)
        a = GetRandomIndex(n)
        for i in range(y):
            v = d.get((a + i) % n)
            lsum += v if v is not None else 0.0
        return lsum
    return udf(lambda c: _sim(c), FloatType())


def udf_ten_window(b_dict):
    """UDF factory: 10-day step window."""
    def _sim(y):
        lsum = 0.0
        d = b_dict.value
        n = len(d)
        a = GetRandomIndex(n)
        for i in range(y):
            v = d.get((a + 10 * i) % n)
            lsum += v if v is not None else 0.0
        return lsum
    return udf(lambda c: _sim(c), FloatType())


def udf_twenty_window(b_dict):
    """UDF factory: 20-day step window."""
    def _sim(y):
        lsum = 0.0
        d = b_dict.value
        n = len(d)
        a = GetRandomIndex(n)
        for i in range(y):
            v = d.get((a + 20 * i) % n)
            lsum += v if v is not None else 0.0
        return lsum
    return udf(lambda c: _sim(c), FloatType())
