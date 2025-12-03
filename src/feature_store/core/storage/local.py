import os
import pandas as pd
from feature_store.core.storage.base import BaseStore

class LocalStore(BaseStore):
    """Implementation of storage on the local filesystem using Parquet"""

    def _ensure_dir(self, path: str):
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)

    def write_dataset(self, df: pd.DataFrame, location: str) -> None:
        self._ensure_dir(location)
        # Write as Parquet (columnar format) for performance
        df.to_parquet(location, engine="pyarrow", index=False)

    def read_dataset(self, location: str) -> pd.DataFrame:
        if not os.path.exists(location):
            raise FileNotFoundError(f"Dataset not found at {location}")
        return pd.read_parquet(location, engine="pyarrow")