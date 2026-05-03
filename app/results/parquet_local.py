import pandas as pd
from datetime import date
from .base import ResultsRepository
from pathlib import Path

class LocalParquetResultsRepository(ResultsRepository):
    def __init__(self, base_path: Path):
        self.base_path = base_path
        
    def load_results(
            self,
            job_id: int,start_date: date,
            end_date: date,):
        path = self.base_path / f"job_id={job_id}"
        df = pd.read_parquet(path)
        mask = (
                (df["business_date"] >= start_date) &
                (df["business_date"] <= end_date)
            )
        
        return df.loc[mask].to_dict(orient="records")