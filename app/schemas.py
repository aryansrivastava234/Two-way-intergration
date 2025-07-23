"""
Pydantic schemas for API request/response models.
"""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, EmailStr, Field


class CustomerBase(BaseModel):
    """Base customer schema with common fields."""
    name: str = Field(..., min_length=1, max_length=255, description="Customer name")
    email: EmailStr = Field(..., description="Customer email address")


class CustomerCreate(CustomerBase):
    """Schema for creating a new customer."""
    pass


class CustomerUpdate(CustomerBase):
    """Schema for updating an existing customer."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    email: Optional[EmailStr] = None


class CustomerResponse(CustomerBase):
    """Schema for customer response."""
    id: int
    stripe_customer_id: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True  # For Pydantic v2 compatibility with SQLAlchemy


class SyncEventResponse(BaseModel):
    """Schema for sync event response."""
    id: int
    entity_type: str
    entity_id: int
    event_type: str
    source_system: str
    integration_name: Optional[str] = None
    external_entity_id: Optional[str] = None
    processed: bool
    error_message: Optional[str] = None
    retry_count: int
    created_at: datetime
    processed_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class APIResponse(BaseModel):
    """Generic API response wrapper."""
    success: bool
    message: str
    data: Optional[dict] = None