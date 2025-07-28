"""
Sync service for handling outbound customer synchronization.
"""
import logging
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.models.customer import Customer, IntegrationMapping
from app.integrations import integration_manager
from app.integrations.base import CustomerData

logger = logging.getLogger(__name__)


class OutboundSyncService:
    """Service for syncing local customers to external systems."""
    
    def __init__(self):
        self.stripe_integration = integration_manager.get_integration('stripe')
    
    def sync_customer_create(self, db: Session, customer: Customer) -> bool:
        """Sync newly created customer to external systems."""
        
        if not self.stripe_integration:
            logger.error("No Stripe integration available")
            return False
        
        try:
            # Prepare customer data for Stripe
            customer_data = CustomerData(
                name=customer.name,
                email=customer.email,
                metadata={
                    'local_id': str(customer.id),
                    'source': 'local_api',
                    'created_via': 'customer_integration_api'
                }
            )
            
            # Create customer in Stripe
            logger.info(f"🔄 Syncing customer {customer.id} to Stripe...")
            result = self.stripe_integration.create_customer(customer_data)
            
            if result.success:
                # Update local customer with Stripe ID
                customer.stripe_customer_id = result.external_id
                
                # Create integration mapping
                mapping = IntegrationMapping(
                    local_entity_id=customer.id,
                    integration_name='stripe',
                    external_entity_id=result.external_id,
                    entity_type='customer',
                    sync_status='active'
                )
                db.add(mapping)
                db.commit()
                
                logger.info(f"✅ Customer {customer.id} synced to Stripe: {result.external_id}")
                print(f"✅ SYNC SUCCESS: Customer {customer.id} → Stripe {result.external_id}")
                return True
            else:
                logger.error(f"❌ Failed to sync customer {customer.id} to Stripe: {result.error_message}")
                print(f"❌ SYNC FAILED: Customer {customer.id} - {result.error_message}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error syncing customer {customer.id}: {e}")
            print(f"❌ SYNC ERROR: Customer {customer.id} - {str(e)}")
            return False
    
    def sync_customer_update(self, db: Session, customer: Customer) -> bool:
        """Sync customer updates to external systems."""
        
        if not self.stripe_integration:
            logger.error("No Stripe integration available")
            return False
        
        if not customer.stripe_customer_id:
            logger.warning(f"Customer {customer.id} has no Stripe ID, treating as create")
            return self.sync_customer_create(db, customer)
        
        try:
            # Prepare updated customer data
            customer_data = CustomerData(
                name=customer.name,
                email=customer.email,
                metadata={
                    'local_id': str(customer.id),
                    'source': 'local_api',
                    'last_updated_via': 'customer_integration_api'
                }
            )
            
            # Update customer in Stripe
            logger.info(f"🔄 Updating customer {customer.id} in Stripe...")
            result = self.stripe_integration.update_customer(
                customer.stripe_customer_id, 
                customer_data
            )
            
            if result.success:
                # Update integration mapping
                mapping = db.query(IntegrationMapping).filter(
                    IntegrationMapping.local_entity_id == customer.id,
                    IntegrationMapping.integration_name == 'stripe',
                    IntegrationMapping.entity_type == 'customer'
                ).first()
                
                if mapping:
                    mapping.sync_status = 'active'
                    mapping.last_synced_at = db.execute(text("SELECT NOW()")).scalar()
                    db.commit()
                
                logger.info(f"✅ Customer {customer.id} updated in Stripe: {customer.stripe_customer_id}")
                print(f"✅ SYNC UPDATE: Customer {customer.id} updated in Stripe")
                return True
            else:
                logger.error(f"❌ Failed to update customer {customer.id} in Stripe: {result.error_message}")
                print(f"❌ SYNC UPDATE FAILED: Customer {customer.id} - {result.error_message}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error updating customer {customer.id}: {e}")
            print(f"❌ SYNC UPDATE ERROR: Customer {customer.id} - {str(e)}")
            return False
    
    def sync_customer_delete(self, db: Session, customer: Customer) -> bool:
        """Sync customer deletion to external systems."""
        
        if not self.stripe_integration:
            logger.error("No Stripe integration available")
            return False
        
        if not customer.stripe_customer_id:
            logger.warning(f"Customer {customer.id} has no Stripe ID, skipping delete sync")
            return True
        
        try:
            # Delete customer from Stripe
            logger.info(f"🔄 Deleting customer {customer.id} from Stripe...")
            result = self.stripe_integration.delete_customer(customer.stripe_customer_id)
            
            if result.success:
                # Update integration mapping status
                mapping = db.query(IntegrationMapping).filter(
                    IntegrationMapping.local_entity_id == customer.id,
                    IntegrationMapping.integration_name == 'stripe',
                    IntegrationMapping.entity_type == 'customer'
                ).first()
                
                if mapping:
                    mapping.sync_status = 'deleted'
                    mapping.last_synced_at = db.execute(text("SELECT NOW()")).scalar()
                    db.commit()
                
                logger.info(f"✅ Customer {customer.id} deleted from Stripe: {customer.stripe_customer_id}")
                print(f"✅ SYNC DELETE: Customer {customer.id} deleted from Stripe")
                return True
            else:
                logger.error(f"❌ Failed to delete customer {customer.id} from Stripe: {result.error_message}")
                print(f"❌ SYNC DELETE FAILED: Customer {customer.id} - {result.error_message}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error deleting customer {customer.id}: {e}")
            print(f"❌ SYNC DELETE ERROR: Customer {customer.id} - {str(e)}")
            return False
    
    def get_sync_status(self, db: Session, customer_id: int) -> dict:
        """Get detailed sync status for a customer."""
        
        customer = db.query(Customer).filter(Customer.id == customer_id).first()
        if not customer:
            return {"error": "Customer not found"}
        
        # Get integration mappings
        mappings = db.query(IntegrationMapping).filter(
            IntegrationMapping.local_entity_id == customer_id,
            IntegrationMapping.entity_type == 'customer'
        ).all()
        
        sync_status = {
            "customer_id": customer_id,
            "local_data": {
                "name": customer.name,
                "email": customer.email,
                "stripe_customer_id": customer.stripe_customer_id,
                "created_at": customer.created_at.isoformat() if customer.created_at else None,
                "updated_at": customer.updated_at.isoformat() if customer.updated_at else None
            },
            "integrations": {}
        }
        
        for mapping in mappings:
            sync_status["integrations"][mapping.integration_name] = {
                "external_id": mapping.external_entity_id,
                "status": mapping.sync_status,
                "last_synced": mapping.last_synced_at.isoformat() if mapping.last_synced_at else None,
                "created_at": mapping.created_at.isoformat() if mapping.created_at else None
            }
        
        return sync_status


# Global sync service instance
outbound_sync_service = OutboundSyncService()