from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from feature_store.core.registry.db import SessionLocal
from feature_store.core.registry.models import Feature, FeatureVersion

class FeatureStore:
    def __init__(self):
        """Initialize the Feature Store Manager"""
        pass

    def register_feature(self, name: str, description: str = "", owner: str = "system") -> Feature:
        """
        Register a new feature definition in the metadata store.
        """
        session: Session = SessionLocal()
        try:
            # Check if feature exists
            existing = session.query(Feature).filter(Feature.name == name).first()
            if existing:
                print(f"Feature '{name}' already exists. Updating metadata...")
                existing.description = description
                existing.owner = owner
                session.commit()
                session.refresh(existing)
                return existing
            
            # Create new feature
            new_feature = Feature(name=name, description=description, owner=owner)
            session.add(new_feature)
            session.commit()
            session.refresh(new_feature)
            print(f"Successfully registered feature: {name}")
            return new_feature
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def list_features(self) -> List[dict]:
        """List all registered features"""
        session: Session = SessionLocal()
        try:
            features = session.query(Feature).all()
            return [
                {
                    "id": f.id,
                    "name": f.name,
                    "owner": f.owner,
                    "versions": len(f.versions)
                } 
                for f in features
            ]
        finally:
            session.close()

    def get_feature(self, name: str) -> Optional[Feature]:
        """Retrieve full feature details by name"""
        session: Session = SessionLocal()
        try:
            return session.query(Feature).filter(Feature.name == name).first()
        finally:
            session.close()