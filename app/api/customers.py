"""
Customer API endpoints with Kafka event publishing.
"""
import logging
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.crud.customer import customer_crud
from app.schemas import CustomerResponse, CustomerCreate, CustomerUpdate
from app.kafka_client import event_publisher
from config import settings

logger = logging.getLogger(__name__)
router = APIRouter()


def publish_customer_event(entity_id: int, event_type: str, customer_data: dict, correlation_id: str = None):
    """Publish customer event to Kafka."""
    try:
        success = event_publisher.publish_event(
            topic=settings.kafka.outbound_topic,
            entity_type='customer',
            entity_id=entity_id,
            event_type=event_type,
            payload=customer_data,
            correlation_id=correlation_id
        )
        if success:
            logger.info(f"Published {event_type} event for customer {entity_id}")
        else:
            logger.error(f"Failed to publish {event_type} event for customer {entity_id}")
        return success
    except Exception as e:
        logger.error(f"Error publishing customer event: {e}")
        return False


@router.post("/", response_model=CustomerResponse, status_code=status.HTTP_201_CREATED)
def create_customer(customer_data: CustomerCreate, db: Session = Depends(get_db)):
    """Create a new customer and publish creation event."""
    try:
        # Create customer in database
        customer = customer_crud.create(db, customer_data)
        
        # Prepare event payload
        event_payload = {
            "name": customer.name,
            "email": customer.email,
            "created_at": customer.created_at.isoformat() if customer.created_at else None,
            "source": "api_create"
        }
        
        # Publish creation event to Kafka
        publish_customer_event(
            entity_id=customer.id,
            event_type="CREATE",
            customer_data=event_payload,
            correlation_id=f"api_create_{customer.id}"
        )
        
        logger.info(f"Created customer {customer.id}: {customer.email}")
        return customer
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating customer: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/", response_model=List[CustomerResponse])
def list_customers(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    """List all customers with pagination."""
    try:
        customers = customer_crud.get_all(db, skip=skip, limit=limit)
        return customers
    except Exception as e:
        logger.error(f"Error listing customers: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/{customer_id}", response_model=CustomerResponse)
def get_customer(customer_id: int, db: Session = Depends(get_db)):
    """Get customer by ID."""
    try:
        customer = customer_crud.get_by_id(db, customer_id)
        if not customer:
            raise HTTPException(status_code=404, detail="Customer not found")
        return customer
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting customer {customer_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/{customer_id}", response_model=CustomerResponse)
def update_customer(customer_id: int, customer_data: CustomerUpdate, db: Session = Depends(get_db)):
    """Update customer and publish update event."""
    try:
        # Get original customer data for comparison
        original_customer = customer_crud.get_by_id(db, customer_id)
        if not original_customer:
            raise HTTPException(status_code=404, detail="Customer not found")
        
        # Update customer in database
        updated_customer = customer_crud.update(db, customer_id, customer_data)
        if not updated_customer:
            raise HTTPException(status_code=404, detail="Customer not found")
        
        # Prepare event payload
        event_payload = {
            "name": updated_customer.name,
            "email": updated_customer.email,
            "stripe_customer_id": updated_customer.stripe_customer_id,
            "updated_at": updated_customer.updated_at.isoformat() if updated_customer.updated_at else None,
            "source": "api_update",
            "previous_data": {
                "name": original_customer.name,
                "email": original_customer.email
            }
        }
        
        # Publish update event to Kafka
        publish_customer_event(
            entity_id=updated_customer.id,
            event_type="UPDATE",
            customer_data=event_payload,
            correlation_id=f"api_update_{updated_customer.id}"
        )
        
        logger.info(f"Updated customer {customer_id}: {updated_customer.email}")
        return updated_customer
        
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error updating customer {customer_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/{customer_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_customer(customer_id: int, db: Session = Depends(get_db)):
    """Delete customer and publish deletion event."""
    try:
        # Get customer data before deletion for event payload
        customer = customer_crud.get_by_id(db, customer_id)
        if not customer:
            raise HTTPException(status_code=404, detail="Customer not found")
        
        # Prepare event payload before deletion
        event_payload = {
            "name": customer.name,
            "email": customer.email,
            "stripe_customer_id": customer.stripe_customer_id,
            "deleted_at": None,  # Will be set when processed
            "source": "api_delete"
        }
        
        # Delete customer from database
        success = customer_crud.delete(db, customer_id)
        if not success:
            raise HTTPException(status_code=404, detail="Customer not found")
        
        # Publish deletion event to Kafka
        publish_customer_event(
            entity_id=customer_id,
            event_type="DELETE",
            customer_data=event_payload,
            correlation_id=f"api_delete_{customer_id}"
        )
        
        logger.info(f"Deleted customer {customer_id}: {customer.email}")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting customer {customer_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/{customer_id}/sync-status")
def get_customer_sync_status(customer_id: int, db: Session = Depends(get_db)):
    """Get customer synchronization status with external systems."""
    try:
        from app.services.sync_service import outbound_sync_service
        sync_status = outbound_sync_service.get_sync_status(db, customer_id)
        return sync_status
    except Exception as e:
        logger.error(f"Error getting sync status for customer {customer_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")