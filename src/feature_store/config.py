import os
from pathlib import Path
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Base Paths
    BASE_DIR: Path = Path(__file__).resolve().parent.parent.parent
    DATA_DIR: Path = BASE_DIR / "data"
    
    # Database
    DB_NAME: str = "feature_store.db"
    
    @property
    def database_url(self) -> str:
        # SQLite for local, can be overridden for cloud
        return f"sqlite:///{self.DATA_DIR}/{self.DB_NAME}"

    @property
    def feature_store_path(self) -> Path:
        return self.DATA_DIR / "features"

    def make_dirs(self):
        os.makedirs(self.DATA_DIR, exist_ok=True)
        os.makedirs(self.feature_store_path, exist_ok=True)

settings = Settings()