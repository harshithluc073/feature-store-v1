from abc import ABC, abstractmethod
import pandas as pd

class BaseStore(ABC):
    """Abstract base class for storage backends (Local, S3, GCS)"""
    
    @abstractmethod
    def write_dataset(self, df: pd.DataFrame, location: str) -> None:
        """Save a pandas DataFrame to a specific location"""
        pass

    @abstractmethod
    def read_dataset(self, location: str) -> pd.DataFrame:
        """Load a pandas DataFrame from a specific location"""
        pass