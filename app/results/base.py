from abc import ABC, abstractmethod
from typing import List, Dict, Any

class ResultsRepository(ABC):
    @abstractmethod
    def load_results(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Return result rows as dicts. Implementations may apply filtering based
        on `params`, which is the persisted Job.params payload (potentially
        merged with caller overrides such as start_date/end_date).
        """
        ...
