from typing import Any, Dict, List, Optional
import pandas as pd
from app.core.config import AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY
from app.results.base import ResultsRepository


class AzureParquetResultsRepository(ResultsRepository):
    """
    Reads Parquet exports from ADLS Gen2 using fsspec/adlfs.

    The repository is constructed with the *full* abfss:// path the writer
    produced (carried on Job.output_ref). For Monte Carlo jobs the layout is:
        .../<prefix>/ticker=<ticker>/
    and rows are filtered down to the (K, T, Runs) combination this job ran.
    """

    def __init__(
        self,
        path: str,
        storage_account: Optional[str] = None,
        storage_key: Optional[str] = None,
    ):
        if not path:
            raise ValueError("path is required")
        self.path = path
        self.storage_account = storage_account or AZURE_STORAGE_ACCOUNT
        self.storage_key = storage_key or AZURE_STORAGE_KEY
        if not self.storage_account:
            raise RuntimeError("AZURE_STORAGE_ACCOUNT is not set")
        if not self.storage_key:
            raise RuntimeError("AZURE_STORAGE_KEY is not set")

    def load_results(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        storage_options = {
            "account_name": self.storage_account,
            "account_key": self.storage_key,
        }
        df = pd.read_parquet(self.path, storage_options=storage_options)
        return _apply_filters(df, params)


def _apply_filters(df: "pd.DataFrame", params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Filter Monte Carlo rows down to the (ticker, K, T, Runs) combination
    represented by `params`. Columns are produced by the
    monte_carlo_simulation notebook; we only filter on what is present.
    """
    for col, key in (
        ("ticker", "ticker"),
        ("K", "strike"),
        ("T", "period_days"),
        ("Runs", "num_simulations"),
    ):
        v = params.get(key)
        if v is not None and col in df.columns:
            df = df[df[col] == v]
    return df.to_dict(orient="records")
