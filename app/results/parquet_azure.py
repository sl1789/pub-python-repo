from datetime import date
from typing import Any, Dict, List
import pandas as pd
from app.core.config import AZURE_STORAGE_ACCOUNT, AZURE_RESULTS_CONTAINER,AZURE_RESULTS_PREFIX, AZURE_STORAGE_KEY
from app.results.base import ResultsRepository

class AzureParquetResultsRepository(ResultsRepository):
    """
    Reads Parquet exports from ADLS Gen2 using fsspec/adlfs.
    Export path pattern:
    abfss://<container>@<account>.dfs.core.windows.net/<prefix>/job_id=<job_id>/
    """
    def __init__(
            self,
            storage_account: str = AZURE_STORAGE_ACCOUNT,
            container: str = AZURE_RESULTS_CONTAINER,
            prefix: str = AZURE_RESULTS_PREFIX,
            storage_key: str = AZURE_STORAGE_KEY,
            ):
        self.storage_account = storage_account
        self.container = container
        self.prefix = prefix
        self.storage_key = storage_key
        if not self.storage_account:
            raise RuntimeError("AZURE_STORAGE_ACCOUNT is not set")
        if not self.storage_key:
            raise RuntimeError("AZURE_STORAGE_KEY is not set")
        
        
    def _path_for_job(self, job_id: int) -> str:
        return (f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net/"f"{self.prefix}/job_id={job_id}")
    
    def load_results(self, job_id: int, start_date: date, end_date: date) -> List[Dict[str,Any]]:
        path = self._path_for_job(job_id)
        storage_options = {
            "account_name": self.storage_account,
            "account_key": self.storage_key,
            }
        df = pd.read_parquet(path, storage_options=storage_options)
        # Ensure business_date is comparable (string vs datetime)
        df["business_date"] = pd.to_datetime(df["business_date"]).dt.date
        mask = (df["business_date"] >= start_date) & (df["business_date"] <= end_date)
        
        return df.loc[mask].to_dict(orient="records")