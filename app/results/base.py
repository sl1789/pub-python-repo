from abc import ABC, abstractmethod
from datetime import date
from typing import List, Dict,Any

class ResultsRepository(ABC):
    @abstractmethod
    def load_results(self,
                    job_id: int,
                    start_date: date,
                    end_date: date,
                    ) -> List[Dict[str,Any]]:
        ...