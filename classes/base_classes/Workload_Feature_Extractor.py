from abc import ABC, abstractmethod
from typing import Dict


class WorkloadFeatureExtractor(ABC):
    """
    Abstract base class for workload feature extractors.

    Subclasses must implement :meth:`extract`, which accepts workload
    source information (e.g. a SQL file path or an XML config path) and
    returns a standardised feature dictionary with the following keys:

        total_statements         : int | float
        table_access_frequency   : dict[str, int | float]
        read_count               : int | float
        write_count              : int | float
        read_write_ratio         : float | None   (None when write_count == 0)
        avg_predicates_per_query : float
        operator_proportions     : dict[str, float]
    """

    @abstractmethod
    def extract(self, *args, **kwargs) -> Dict:
        """
        Extract workload features from the provided source.

        Returns
        -------
        dict
            A feature dictionary with the standardised keys listed in the
            class docstring.
        """
