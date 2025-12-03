from feature_store.core.storage.local import LocalStore

def get_artifact_store():
    """
    Factory function to get the storage backend.
    In the future, this can check config.py to return S3Store or GCSStore.
    """
    return LocalStore()