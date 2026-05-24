"""Smoke test for the Streamlit Analysis page.

We use `streamlit.testing.v1.AppTest` to render the page in-process with
the API client fully mocked, so no network/auth is required. The goal
is to catch import errors, layout exceptions, and obvious key/column
mistakes — not to validate chart pixels.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

st_testing = pytest.importorskip("streamlit.testing.v1")
AppTest = st_testing.AppTest


PAGE = (
    Path(__file__).resolve().parents[1] / "ui" / "pages" / "5_Analysis.py"
)


def _mva_payload() -> dict:
    return {
        "dataset": "mc_vs_actual",
        "ticker": "AAPL",
        "ds_updated_at": "2025-09-02T12:00:00",
        "rows": [
            {"method": "historical", "num_runs": 1000, "scale_label": "1k",
             "moneyness": 1.0, "mc_call": 5.1, "actual_call": 5.0,
             "mc_put": 1.05, "actual_put": 1.0, "is_atm": True},
            {"method": "window", "num_runs": 1000, "scale_label": "1k",
             "moneyness": 1.05, "mc_call": 5.0, "actual_call": 5.0,
             "mc_put": 1.00, "actual_put": 1.0, "is_atm": False},
        ],
    }


def _emc_join_payload() -> dict:
    return {
        "ticker": "AAPL",
        "num_runs": 1_000_000,
        "ds_updated_at": "2025-09-02T12:00:00",
        "rows": [
            {"method": "historical", "side": "call",
             "pre_mape": 20.0, "post_mape": 5.0, "delta": -15.0},
            {"method": "historical", "side": "put",
             "pre_mape": 18.0, "post_mape": 4.0, "delta": -14.0},
        ],
    }


def _block_payload() -> dict:
    return {
        "dataset": "block_length_sweep",
        "ticker": "AAPL",
        "ds_updated_at": "2025-09-02T12:00:00",
        "rows": [
            {"block_mean_len": 5, "side": "call", "abs_pct_err": 10.0},
            {"block_mean_len": 10, "side": "call", "abs_pct_err": 4.0},
            {"block_mean_len": 5, "side": "put", "abs_pct_err": 12.0},
            {"block_mean_len": 10, "side": "put", "abs_pct_err": 6.0},
        ],
    }


def _lam_payload() -> dict:
    return {
        "dataset": "lam_sweep",
        "ticker": "AAPL",
        "ds_updated_at": "2025-09-02T12:00:00",
        "rows": [
            {"lam": 0.94, "side": "call", "abs_pct_err": 8.0},
            {"lam": 0.97, "side": "call", "abs_pct_err": 5.0},
            {"lam": 0.94, "side": "put", "abs_pct_err": 9.0},
            {"lam": 0.97, "side": "put", "abs_pct_err": 6.0},
        ],
    }


def _get_dataset(token, name, ticker=None):
    if name == "mc_vs_actual":
        return _mva_payload()
    if name == "block_length_sweep":
        return _block_payload()
    if name == "lam_sweep":
        return _lam_payload()
    raise AssertionError(f"unexpected dataset {name!r}")


def test_analysis_page_renders_without_error():
    with patch("api_client.get_token_or_stop", return_value="fake-token"), \
         patch("api_client.get_dataset", side_effect=_get_dataset), \
         patch("api_client.get_emc_join", return_value=_emc_join_payload()), \
         patch("api_client.render_session_sidebar"):
        at = AppTest.from_file(str(PAGE), default_timeout=30).run()

    # No uncaught Streamlit exceptions.
    assert not at.exception, [str(e.value) for e in at.exception]
    # Header is present.
    assert any("Analysis" in t.value for t in at.title)
