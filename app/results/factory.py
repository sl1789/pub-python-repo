from app.results.base import ResultsRepository
from app.results.parquet_azure import AzureParquetResultsRepository


def get_results_repository(output_ref: str | None) -> ResultsRepository:
    """Factory based on output_ref scheme.

    Supported:
        parquet:abfss://...      -> AzureParquetResultsRepository(path=...)
    """
    if not output_ref:
        raise ValueError("output_ref is required to load results")
    if not output_ref.startswith("parquet:"):
        raise ValueError(f"Unsupported output_ref scheme: {output_ref}")

    path = output_ref[len("parquet:"):]
    if not path.startswith("abfss://"):
        raise ValueError(f"Unsupported parquet path: {path}")
    return AzureParquetResultsRepository(path=path)
