"""
Custom Stripe integration using requests library.
Works around issues with the official stripe library.
"""
import logging
import requests
from typing import Dict, Any, List, Optional
from datetime import datetime

from .base import BaseIntegration, CustomerData, SyncResult

logger = logging.getLogger(__name__)


class CustomStripeIntegration(BaseIntegration):
    """Custom Stripe integration using requests library."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        self.api_key = config.get('secret_key')
        self.base_url = "https://api.stripe.com/v1"
        
        if not self.api_key:
            logger.error("Stripe API key not provided")
            self.stripe_available = False
            return
            
        if not self.api_key.startswith('sk_'):
            logger.error("Invalid Stripe API key format")
            self.stripe_available = False
            return
            
        self.stripe_available = True
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        logger.info("Custom Stripe integration initialized")
    
    def create_customer(self, customer_data: CustomerData) -> SyncResult:
        """Create a customer in Stripe."""
        if not self.stripe_available:
            return SyncResult(success=False, error_message="Stripe not available")
        
        try:
            data = {
                "name": customer_data.name,
                "email": customer_data.email,
            }
            
            # Add metadata if provided
            if customer_data.metadata:
                for key, value in customer_data.metadata.items():
                    data[f"metadata[{key}]"] = str(value)
            
            response = requests.post(
                f"{self.base_url}/customers",
                headers=self.headers,
                data=data
            )
            
            if response.status_code == 200:
                customer = response.json()
                logger.info(f"Created Stripe customer: {customer['id']}")
                
                return SyncResult(
                    success=True,
                    external_id=customer['id'],
                    data={
                        'stripe_customer': customer,
                        'created_at': datetime.utcnow().isoformat()
                    }
                )
            else:
                error_msg = f"Stripe API error: {response.status_code} - {response.text}"
                logger.error(error_msg)
                return SyncResult(success=False, error_message=error_msg)
                
        except Exception as e:
            logger.error(f"Error creating Stripe customer: {e}")
            return SyncResult(success=False, error_message=str(e))
    
    def update_customer(self, external_id: str, customer_data: CustomerData) -> SyncResult:
        """Update a customer in Stripe."""
        if not self.stripe_available:
            return SyncResult(success=False, error_message="Stripe not available")
        
        try:
            data = {
                "name": customer_data.name,
                "email": customer_data.email,
            }
            
            # Add metadata if provided
            if customer_data.metadata:
                for key, value in customer_data.metadata.items():
                    data[f"metadata[{key}]"] = str(value)
            
            response = requests.post(
                f"{self.base_url}/customers/{external_id}",
                headers=self.headers,
                data=data
            )
            
            if response.status_code == 200:
                customer = response.json()
                logger.info(f"Updated Stripe customer: {external_id}")
                
                return SyncResult(
                    success=True,
                    external_id=external_id,
                    data={'stripe_customer': customer}
                )
            else:
                error_msg = f"Stripe API error: {response.status_code} - {response.text}"
                logger.error(error_msg)
                return SyncResult(success=False, error_message=error_msg)
                
        except Exception as e:
            logger.error(f"Error updating Stripe customer: {e}")
            return SyncResult(success=False, error_message=str(e))
    
    def delete_customer(self, external_id: str) -> SyncResult:
        """Delete a customer from Stripe."""
        if not self.stripe_available:
            return SyncResult(success=False, error_message="Stripe not available")
        
        try:
            response = requests.delete(
                f"{self.base_url}/customers/{external_id}",
                headers=self.headers
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"Deleted Stripe customer: {external_id}")
                
                return SyncResult(
                    success=True,
                    external_id=external_id,
                    data={'deleted': result.get('deleted', True)}
                )
            else:
                error_msg = f"Stripe API error: {response.status_code} - {response.text}"
                logger.error(error_msg)
                return SyncResult(success=False, error_message=error_msg)
                
        except Exception as e:
            logger.error(f"Error deleting Stripe customer: {e}")
            return SyncResult(success=False, error_message=str(e))
    
    def get_customer(self, external_id: str) -> SyncResult:
        """Get a customer from Stripe."""
        if not self.stripe_available:
            return SyncResult(success=False, error_message="Stripe not available")
        
        try:
            response = requests.get(
                f"{self.base_url}/customers/{external_id}",
                headers=self.headers
            )
            
            if response.status_code == 200:
                customer = response.json()
                
                customer_data = CustomerData(
                    name=customer.get('name', ''),
                    email=customer.get('email', ''),
                    external_id=customer['id'],
                    metadata=customer.get('metadata', {})
                )
                
                return SyncResult(
                    success=True,
                    external_id=external_id,
                    data={
                        'customer_data': customer_data,
                        'stripe_customer': customer
                    }
                )
            else:
                error_msg = f"Stripe API error: {response.status_code} - {response.text}"
                logger.error(error_msg)
                return SyncResult(success=False, error_message=error_msg)
                
        except Exception as e:
            logger.error(f"Error retrieving Stripe customer: {e}")
            return SyncResult(success=False, error_message=str(e))
    
    def get_customers_since(self, timestamp: str) -> List[CustomerData]:
        """Get customers modified since timestamp."""
        if not self.stripe_available:
            return []
        
        try:
            # Convert timestamp to Unix timestamp
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            unix_timestamp = int(dt.timestamp())
            
            params = {
                'created[gte]': unix_timestamp,
                'limit': 100
            }
            
            response = requests.get(
                f"{self.base_url}/customers",
                headers=self.headers,
                params=params
            )
            
            if response.status_code == 200:
                result = response.json()
                customers = result.get('data', [])
                
                customer_list = []
                for customer in customers:
                    customer_data = CustomerData(
                        name=customer.get('name', ''),
                        email=customer.get('email', ''),
                        external_id=customer['id'],
                        metadata=customer.get('metadata', {})
                    )
                    customer_list.append(customer_data)
                
                logger.info(f"Retrieved {len(customer_list)} customers from Stripe since {timestamp}")
                return customer_list
            else:
                logger.error(f"Failed to retrieve customers: {response.status_code} - {response.text}")
                return []
                
        except Exception as e:
            logger.error(f"Error retrieving customers from Stripe: {e}")
            return []
    
    def test_connection(self) -> bool:
        """Test connection to Stripe."""
        if not self.stripe_available:
            return False
        
        try:
            response = requests.get(
                f"{self.base_url}/customers?limit=1",
                headers=self.headers
            )
            
            if response.status_code == 200:
                logger.info("Stripe connection test successful")
                return True
            else:
                logger.error(f"Stripe connection test failed: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Stripe connection test failed: {e}")
            return False


class MockStripeIntegration(BaseIntegration):
    """Mock Stripe integration for testing without API keys."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.mock_customers = {}
        self.next_id = 1
        logger.info("Mock Stripe integration initialized")
    
    def create_customer(self, customer_data: CustomerData) -> SyncResult:
        """Mock create customer."""
        customer_id = f"cus_mock_{self.next_id:08d}"
        self.next_id += 1
        
        self.mock_customers[customer_id] = {
            'id': customer_id,
            'name': customer_data.name,
            'email': customer_data.email,
            'metadata': customer_data.metadata or {},
            'created': datetime.utcnow().isoformat()
        }
        
        print(f" MOCK STRIPE: Created customer {customer_id}")
        print(f"   Name: {customer_data.name}")
        print(f"   Email: {customer_data.email}")
        
        return SyncResult(
            success=True,
            external_id=customer_id,
            data={'mock_customer': self.mock_customers[customer_id]}
        )
    
    def update_customer(self, external_id: str, customer_data: CustomerData) -> SyncResult:
        """Mock update customer."""
        if external_id not in self.mock_customers:
            return SyncResult(success=False, error_message="Customer not found")
        
        self.mock_customers[external_id].update({
            'name': customer_data.name,
            'email': customer_data.email,
            'metadata': customer_data.metadata or {}
        })
        
        print(f" MOCK STRIPE: Updated customer {external_id}")
        return SyncResult(success=True, external_id=external_id)
    
    def delete_customer(self, external_id: str) -> SyncResult:
        """Mock delete customer."""
        if external_id not in self.mock_customers:
            return SyncResult(success=False, error_message="Customer not found")
        
        del self.mock_customers[external_id]
        print(f" MOCK STRIPE: Deleted customer {external_id}")
        return SyncResult(success=True, external_id=external_id)
    
    def get_customer(self, external_id: str) -> SyncResult:
        """Mock get customer."""
        if external_id not in self.mock_customers:
            return SyncResult(success=False, error_message="Customer not found")
        
        customer = self.mock_customers[external_id]
        customer_data = CustomerData(
            name=customer['name'],
            email=customer['email'],
            external_id=customer['id'],
            metadata=customer['metadata']
        )
        
        return SyncResult(
            success=True,
            external_id=external_id,
            data={'customer_data': customer_data}
        )
    
    def get_customers_since(self, timestamp: str) -> List[CustomerData]:
        """Mock get customers since timestamp."""
        customers = []
        for customer in self.mock_customers.values():
            customers.append(CustomerData(
                name=customer['name'],
                email=customer['email'],
                external_id=customer['id'],
                metadata=customer['metadata']
            ))
        return customers
    
    def test_connection(self) -> bool:
        """Mock test connection."""
        print("🎭 MOCK STRIPE: Connection test successful")
        return True


# Use the custom integration as the main one
StripeIntegration = CustomStripeIntegration