"""
Fixed config/__init__.py - only imports what exists.
"""
from .settings import settings

# Only export what actually exists
__all__ = ["settings"]