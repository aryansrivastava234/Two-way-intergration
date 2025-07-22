"""
Customer model and related database schemas.
"""
from datetime import datetime
from typing import Optional
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text, Index
from sqlalchemy.sql import func
from app.database import Base


class Customer(Base):
    """Customer model for local customer catalog."""
    
    __tablename__ = "customers"
    
    # Primary key
    id = Column(Integer, primary_key=True, index=True)
    
    # Customer information
    name = Column(String(255), nullable=False)
    email = Column(String(255), nullable=False, unique=True, index=True)
    
    # External system mappings
    stripe_customer_id = Column(String(100), unique=True, nullable=True, index=True)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    def __repr__(self):
        return f"<Customer(id={self.id}, email='{self.email}', name='{self.name}')>"


class SyncEvent(Base):
    """Track synchronization events between systems."""
    
    __tablename__ = "sync_events"
    
    # Primary key
    id = Column(Integer, primary_key=True, index=True)
    
    # Event details
    entity_type = Column(String(50), nullable=False, index=True)  
    entity_id = Column(Integer, nullable=False, index=True)       
    event_type = Column(String(50), nullable=False)              
    source_system = Column(String(50), nullable=False)           
    
    # Integration details
    integration_name = Column(String(50), nullable=True)        
    external_entity_id = Column(String(100), nullable=True)      
    
    # Event payload and status
    payload = Column(Text, nullable=True)                        
    processed = Column(Boolean, default=False, index=True)
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, default=0)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    processed_at = Column(DateTime(timezone=True), nullable=True)
    
    # Add composite indexes for better query performance
    __table_args__ = (
        Index('idx_entity_type_processed', 'entity_type', 'processed'),
        Index('idx_source_system_created', 'source_system', 'created_at'),
    )
    
    def __repr__(self):
        return (f"<SyncEvent(id={self.id}, entity_type='{self.entity_type}', "
                f"event_type='{self.event_type}', processed={self.processed})>")


class IntegrationMapping(Base):
    """Map local entities to external system entities."""
    
    __tablename__ = "integration_mappings"
    
    # Primary key
    id = Column(Integer, primary_key=True, index=True)
    
    # Mapping details
    local_entity_id = Column(Integer, nullable=False, index=True)
    integration_name = Column(String(50), nullable=False, index=True) 
    external_entity_id = Column(String(100), nullable=False)
    entity_type = Column(String(50), nullable=False)                  
    
    # Metadata
    sync_status = Column(String(20), default='active') 
    last_synced_at = Column(DateTime(timezone=True), nullable=True)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    # Composite unique constraint
    __table_args__ = (
        Index('idx_unique_mapping', 'local_entity_id', 'integration_name', 'entity_type', unique=True),
    )
    
    def __repr__(self):
        return (f"<IntegrationMapping(local_id={self.local_entity_id}, "
                f"integration='{self.integration_name}', external_id='{self.external_entity_id}')>")