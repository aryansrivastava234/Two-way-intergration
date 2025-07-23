"""
Main FastAPI application.
"""
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List

from app.database import get_db
from app.schemas import CustomerCreate, CustomerUpdate, CustomerResponse, APIResponse
from app.crud import customer_crud
from app.kafka_client import event_publisher
from config import settings 
# Create FastAPI app
app = FastAPI(
    title="Zenskar Customer Integration API",
    description="Two-way customer sync between local system and external services",
    version="1.0.0",
    debug=settings.debug
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", response_model=APIResponse)
async def root():
    """Root endpoint."""
    return APIResponse(
        success=True,
        message="Customer Integration API is running",
        data={"version": "1.0.0"}
    )


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "timestamp": "2025-07-23T10:30:00Z"}


@app.post("/customers", response_model=CustomerResponse, status_code=status.HTTP_201_CREATED)
async def create_customer(
    customer_data: CustomerCreate,
    db: Session = Depends(get_db)
):
    """Create a new customer."""
    try:
        # Create customer in database
        customer = customer_crud.create(db, customer_data)
        
        # Publish event to Kafka
        event_payload = {
            "id": customer.id,
            "name": customer.name,
            "email": customer.email
        }
        
        event_published = event_publisher.publish_event(
            topic=settings.kafka.outbound_topic,
            entity_type="customer",
            entity_id=customer.id,
            event_type="CREATE",
            payload=event_payload
        )
        
        if not event_published:
            # Log warning but don't fail the request
            print(f"Warning: Failed to publish CREATE event for customer {customer.id}")
            
        return customer
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/customers", response_model=List[CustomerResponse])
async def get_customers(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """Get all customers with pagination."""
    customers = customer_crud.get_all(db, skip=skip, limit=limit)
    return customers


@app.get("/customers/{customer_id}", response_model=CustomerResponse)
async def get_customer(customer_id: int, db: Session = Depends(get_db)):
    """Get customer by ID."""
    customer = customer_crud.get_by_id(db, customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    return customer


@app.put("/customers/{customer_id}", response_model=CustomerResponse)
async def update_customer(
    customer_id: int,
    customer_data: CustomerUpdate,
    db: Session = Depends(get_db)
):
    """Update customer."""
    try:
        customer = customer_crud.update(db, customer_id, customer_data)
        if not customer:
            raise HTTPException(status_code=404, detail="Customer not found")
            
        # Publish update event
        event_payload = {
            "id": customer.id,
            "name": customer.name,
            "email": customer.email,
            "stripe_customer_id": customer.stripe_customer_id
        }
        
        event_publisher.publish_event(
            topic=settings.kafka.outbound_topic,
            entity_type="customer",
            entity_id=customer.id,
            event_type="UPDATE",
            payload=event_payload
        )
        
        return customer
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/customers/{customer_id}", response_model=APIResponse)
async def delete_customer(customer_id: int, db: Session = Depends(get_db)):
    """Delete customer."""
    # Get customer data before deletion for event payload
    customer = customer_crud.get_by_id(db, customer_id)
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    
    # Delete customer
    success = customer_crud.delete(db, customer_id)
    if not success:
        raise HTTPException(status_code=404, detail="Customer not found")
    
    # Publish delete event
    event_payload = {
        "id": customer.id,
        "name": customer.name,
        "email": customer.email,
        "stripe_customer_id": customer.stripe_customer_id
    }
    
    event_publisher.publish_event(
        topic=settings.kafka.outbound_topic,
        entity_type="customer",
        entity_id=customer.id,
        event_type="DELETE",
        payload=event_payload
    )
    
    return APIResponse(
        success=True,
        message=f"Customer {customer_id} deleted successfully"
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug
    )