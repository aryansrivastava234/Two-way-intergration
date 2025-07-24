"""
Base integration classes for external system integrations.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from dataclasses import dataclass


@dataclass
class CustomerData:
    """Standard customer data structure for integrations."""
    name: str
    email: str
    external_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class SyncResult:
    """Result of a sync operation."""
    success: bool
    external_id: Optional[str] = None
    error_message: Optional[str] = None
    data: Optional[Dict[str, Any]] = None


class BaseIntegration(ABC):
    """Base class for all external system integrations."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.integration_name = self.__class__.__name__.lower().replace('integration', '')
    
    @abstractmethod
    def create_customer(self, customer_data: CustomerData) -> SyncResult:
        """Create a customer in the external system."""
        pass
    
    @abstractmethod
    def update_customer(self, external_id: str, customer_data: CustomerData) -> SyncResult:
        """Update a customer in the external system."""
        pass
    
    @abstractmethod
    def delete_customer(self, external_id: str) -> SyncResult:
        """Delete a customer from the external system."""
        pass
    
    @abstractmethod
    def get_customer(self, external_id: str) -> SyncResult:
        """Get a customer from the external system."""
        pass
    
    @abstractmethod
    def get_customers_since(self, timestamp: str) -> List[CustomerData]:
        """Get customers modified since timestamp."""
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Test connection to the external system."""
        pass