"""Tests for the /datasets routes (research outputs of mc_vs_actual_test).

We mock `AzureParquetResultsRepository.load_results` to return canned
pandas-frame rows so the tests don't need real ADLS credentials or a
Spark runtime. The mocking is at the *result* layer (after the repo
has decided where to read from), which means the path-building logic
in `databricks/lib/paths` is exercised for free.
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def _auth_headers() -> dict:
    r = client.post("/auth/token", data={"username": "demo", "password": "demo123"})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


# Common env so AzureParquetResultsRepository.__init__ doesn't reject.
_AZURE_ENV = {
    "AZURE_STORAGE_ACCOUNT": "acct",
    "AZURE_STORAGE_KEY": "key",
    "AZURE_RESULTS_CONTAINER": "results",
    "AZURE_RESULTS_PREFIX": "export",
}


# ---------------------------------------------------------------------------
# /datasets (catalog listing)
# ---------------------------------------------------------------------------
def test_list_datasets_returns_research_only():
    r = client.get("/datasets", headers=_auth_headers())
    assert r.status_code == 200, r.text
    names = {item["name"] for item in r.json()["items"]}
    # Must include the 4 research datasets.
    assert {"mc_vs_actual", "emc_diagnostics", "block_length_sweep", "lam_sweep"} <= names
    # Must NOT include the per-job/non-UI datasets.
    assert "simulations" not in names
    assert "options" not in names
    assert "scalability" not in names


# ---------------------------------------------------------------------------
# /datasets/{name}
# ---------------------------------------------------------------------------
@patch.dict("os.environ", _AZURE_ENV, clear=False)
def test_read_dataset_requires_ticker_for_partitioned():
    # mc_vs_actual is ticker-partitioned; omitting `ticker` must 400.
    r = client.get("/datasets/mc_vs_actual", headers=_auth_headers())
    assert r.status_code == 400
    assert "ticker" in r.json()["detail"].lower()


@patch.dict("os.environ", _AZURE_ENV, clear=False)
@patch("app.api.datasets.AzureParquetResultsRepository")
def test_read_dataset_returns_rows_and_updated_at(mock_repo_cls):
    mock_repo_cls.return_value.load_results.return_value = [
        {"ticker": "AAPL", "K": 150.0, "T": 10, "mc_call": 5.0,
         "actual_call": 5.1, "created_at": "2025-09-01T12:00:00"},
        {"ticker": "AAPL", "K": 150.0, "T": 10, "mc_call": 5.2,
         "actual_call": 5.1, "created_at": "2025-09-01T13:00:00"},
    ]
    r = client.get(
        "/datasets/mc_vs_actual",
        params={"ticker": "AAPL"},
        headers=_auth_headers(),
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["dataset"] == "mc_vs_actual"
    assert body["ticker"] == "AAPL"
    assert len(body["rows"]) == 2
    # max(created_at) should be the later stamp.
    assert body["ds_updated_at"].startswith("2025-09-01T13:00:00")


@patch.dict("os.environ", _AZURE_ENV, clear=False)
def test_read_unknown_dataset_returns_404():
    r = client.get("/datasets/does_not_exist", headers=_auth_headers())
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# /datasets/mc_vs_actual_emc_join
# ---------------------------------------------------------------------------
@patch.dict("os.environ", _AZURE_ENV, clear=False)
@patch("app.api.datasets.AzureParquetResultsRepository")
def test_emc_join_returns_pre_post_deltas(mock_repo_cls):
    """The join takes pre-EMC rows from mc_vs_actual (at num_runs == emc_scale,
    same methods as EMC) and post-EMC rows from emc_diagnostics, then
    aggregates MAPE per (method, side) and emits one row per pair.
    """

    # EMC notebook re-runs at one scale; here that scale is 1,000,000.
    EMC_SCALE = 1_000_000

    # The route loads emc_diagnostics first, then mc_vs_actual; mock both.
    emc_rows = [
        # method=historical: post-EMC MAPE on calls is 5% (|5-5.25|/5 * 100)
        {"method": "historical", "num_runs": EMC_SCALE, "mc_call": 5.25,
         "mc_put": 1.05, "actual_call": 5.0, "actual_put": 1.0,
         "created_at": "2025-09-02T10:00:00"},
        # method=window
        {"method": "window", "num_runs": EMC_SCALE, "mc_call": 5.10,
         "mc_put": 1.02, "actual_call": 5.0, "actual_put": 1.0,
         "created_at": "2025-09-02T10:00:00"},
    ]
    mva_rows = [
        # Pre-EMC at the same scale: 20% off on historical calls, 4% on window.
        {"method": "historical", "num_runs": EMC_SCALE, "mc_call": 6.0,
         "mc_put": 1.20, "actual_call": 5.0, "actual_put": 1.0,
         "created_at": "2025-09-02T09:00:00"},
        {"method": "window", "num_runs": EMC_SCALE, "mc_call": 5.20,
         "mc_put": 1.04, "actual_call": 5.0, "actual_put": 1.0,
         "created_at": "2025-09-02T09:00:00"},
        # A different scale that must be filtered out.
        {"method": "historical", "num_runs": 1000, "mc_call": 99.0,
         "mc_put": 99.0, "actual_call": 5.0, "actual_put": 1.0,
         "created_at": "2025-09-02T09:00:00"},
    ]

    def _side_effect(*args, **kwargs):
        # Distinguish the two calls by the `path` argument.
        path = kwargs.get("path") or (args[0] if args else "")
        repo = mock_repo_cls.return_value
        if "emc_diagnostics" in path:
            repo.load_results.return_value = emc_rows
        else:
            repo.load_results.return_value = mva_rows
        return repo

    mock_repo_cls.side_effect = _side_effect

    r = client.get(
        "/datasets/mc_vs_actual_emc_join",
        params={"ticker": "AAPL"},
        headers=_auth_headers(),
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ticker"] == "AAPL"
    assert body["num_runs"] == EMC_SCALE
    # 2 methods x 2 sides = 4 rows.
    assert len(body["rows"]) == 4

    by_key = {(row["method"], row["side"]): row for row in body["rows"]}

    # historical call: pre=20%, post=5%, delta=-15
    pre_post = by_key[("historical", "call")]
    assert pre_post["pre_mape"] == pytest.approx(20.0, abs=0.01)
    assert pre_post["post_mape"] == pytest.approx(5.0, abs=0.01)
    assert pre_post["delta"] == pytest.approx(-15.0, abs=0.01)

    # window call: pre=4%, post=2%, delta=-2
    pre_post = by_key[("window", "call")]
    assert pre_post["pre_mape"] == pytest.approx(4.0, abs=0.01)
    assert pre_post["post_mape"] == pytest.approx(2.0, abs=0.01)
    assert pre_post["delta"] == pytest.approx(-2.0, abs=0.01)


@patch.dict("os.environ", _AZURE_ENV, clear=False)
def test_emc_join_requires_ticker():
    r = client.get("/datasets/mc_vs_actual_emc_join", headers=_auth_headers())
    assert r.status_code == 422  # FastAPI validation: missing required query param
