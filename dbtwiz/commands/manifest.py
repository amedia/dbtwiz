from dbtwiz.manifest import Manifest

def manifest(type: str):
    """Update manifests (dev and or production)"""
    Manifest.update_manifests(type)
