from app.results.base import ResultsRepository
from app.results.parquet_azure import AzureParquetResultsRepository

def get_results_repository(output_ref: str | None) -> ResultsRepository:
    """
    Factory based on output_ref scheme.For now we support:
    parquet:... -> AzureParquetResultsRepository
    """
    
    if not output_ref:
        # If missing, default to Azure parquet for this architecture
        return AzureParquetResultsRepository()
    
    # Very simple scheme parsing
    # Example: parquet:abfss://.../export/job_id=123
    if output_ref.startswith("parquet:"):
        return AzureParquetResultsRepository()
    
    raise ValueError(f"Unsupported output_ref scheme: {output_ref}")