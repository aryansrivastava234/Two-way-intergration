"""
Inbound Kafka Worker - Processes Stripe webhook events and syncs to local DB.
Phase 6: Real Kafka workers with conflict resolution.
"""
import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import SessionLocal
from app.kafka_client import create_consumer
from app.crud.customer import customer_crud
from app.models.customer import Customer, IntegrationMapping
from app.schemas import CustomerCreate, CustomerUpdate
from config import settings

logger = logging.getLogger(__name__)


class InboundKafkaWorker:
    """Processes inbound sync events from Stripe webhooks via Kafka queue."""
    
    def __init__(self, group_id: str = "inbound_sync_group"):
        self.group_id = group_id
        self.consumer = None
        self.running = False
        self.processed_count = 0
        self.error_count = 0
        self.duplicate_count = 0
        
    def start_worker(self):
        """Start the inbound worker."""
        logger.info("🚀 Starting inbound Kafka worker...")
        
        # Create Kafka consumer
        topics = [settings.kafka.inbound_topic]
        self.consumer = create_consumer(topics, self.group_id)
        
        # Start consuming messages
        self.consumer.start_consuming(
            message_handler=self.process_inbound_message,
            error_handler=self.handle_processing_error
        )
        
        self.running = True
        logger.info(f"✅ Inbound worker started, consuming from: {topics}")
        print(f"🔄 INBOUND WORKER: Listening for Stripe → local sync events")
        
        # Keep worker running
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("⏸️ Received stop signal")
        finally:
            self.stop_worker()
    
    def stop_worker(self):
        """Stop the inbound worker."""
        logger.info("🛑 Stopping inbound worker...")
        self.running = False
        
        if self.consumer:
            self.consumer.stop_consuming()
        
        logger.info(f"✅ Inbound worker stopped. Processed: {self.processed_count}, Errors: {self.error_count}, Duplicates: {self.duplicate_count}")
    
    def process_inbound_message(self, message: Dict[str, Any]) -> bool:
        """Process a single inbound sync message."""
        try:
            event_id = message.get('event_id', 'unknown')
            entity_type = message.get('entity_type')
            event_type = message.get('event_type')
            payload = message.get('payload', {})
            
            logger.info(f"📥 Processing inbound event: {event_id} ({event_type})")
            print(f"📥 INBOUND: Processing {event_type} from Stripe")
            
            # Validate message structure
            if not all([entity_type, event_type, payload]):
                logger.error(f"❌ Invalid message structure: {event_id}")
                return False
            
            # Only process customer events
            if entity_type != 'customer':
                logger.debug(f"⏭️ Ignoring non-customer event: {entity_type}")
                return True
            
            # Check for duplicates using Stripe event ID
            stripe_event_id = payload.get('stripe_event_id')
            if stripe_event_id and self._is_duplicate_event(stripe_event_id):
                logger.info(f"⏭️ Duplicate event detected: {stripe_event_id}")
                self.duplicate_count += 1
                return True
            
            # Check conflict resolution
            conflict_info = payload.get('conflict_resolution', {})
            if conflict_info and not self._should_process_event(conflict_info):
                logger.info(f"⏭️ Skipping event due to conflict resolution: {event_id}")
                return True
            
            # Process based on event type
            success = False
            db = SessionLocal()
            try:
                if event_type == 'INBOUND_CREATE':
                    success = self._handle_customer_create(db, payload)
                elif event_type == 'INBOUND_UPDATE':
                    success = self._handle_customer_update(db, payload)
                elif event_type == 'INBOUND_DELETE':
                    success = self._handle_customer_delete(db, payload)
                else:
                    logger.warning(f"⚠️ Unknown event type: {event_type}")
                    return True
                
                # Mark event as processed if successful
                if success and stripe_event_id:
                    self._mark_event_processed(db, stripe_event_id)
                
            finally:
                db.close()
            
            if success:
                self.processed_count += 1
                logger.info(f"✅ Successfully processed inbound event: {event_id}")
                print(f"✅ INBOUND: {event_type} completed")
            else:
                self.error_count += 1
                logger.error(f"❌ Failed to process inbound event: {event_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Error processing inbound message: {e}")
            self.error_count += 1
            return False
    
    def _should_process_event(self, conflict_info: Dict[str, Any]) -> bool:
        """Determine if event should be processed based on conflict resolution."""
        try:
            # For inbound events from Stripe webhooks, we generally want to process them
            # as they represent the source of truth from Stripe
            source_priority = conflict_info.get('source_priority', 'stripe')
            timestamp_str = conflict_info.get('timestamp')
            
            if not timestamp_str:
                return True
            
            # Check if event is too old (> 10 minutes)
            event_timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            age_seconds = (datetime.utcnow() - event_timestamp.replace(tzinfo=None)).total_seconds()
            
            if age_seconds > 600:  # 10 minutes
                logger.warning(f"⚠️ Inbound event is too old ({age_seconds}s), skipping")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error in conflict resolution: {e}")
            return True
    
    def _is_duplicate_event(self, stripe_event_id: str) -> bool:
        """Check if Stripe event has already been processed."""
        db = SessionLocal()
        try:
            # Check in processed events table (we'll create this)
            result = db.execute(
                text("SELECT 1 FROM processed_stripe_events WHERE stripe_event_id = :event_id"),
                {"event_id": stripe_event_id}
            ).fetchone()
            
            return result is not None
            
        except Exception:
            # Table might not exist yet, assume not duplicate
            return False
        finally:
            db.close()
    
    def _mark_event_processed(self, db: Session, stripe_event_id: str):
        """Mark Stripe event as processed to prevent duplicates."""
        try:
            db.execute(
                text("""
                    INSERT INTO processed_stripe_events (stripe_event_id, processed_at, created_at) 
                    VALUES (:event_id, NOW(), NOW())
                    ON CONFLICT (stripe_event_id) DO NOTHING
                """),
                {"event_id": stripe_event_id}
            )
            db.commit()
            
        except Exception as e:
            logger.error(f"❌ Failed to mark event as processed: {e}")
            db.rollback()
    
    def _handle_customer_create(self, db: Session, payload: Dict[str, Any]) -> bool:
        """Handle customer creation from Stripe."""
        try:
            stripe_customer_id = payload.get('stripe_customer_id')
            name = payload.get('name', '')
            email = payload.get('email', '')
            
            if not stripe_customer_id or not email:
                logger.error("❌ Missing required fields for customer creation")
                return False
            
            # Check if customer already exists by Stripe ID
            existing_mapping = db.query(IntegrationMapping).filter(
                IntegrationMapping.external_entity_id == stripe_customer_id,
                IntegrationMapping.integration_name == 'stripe',
                IntegrationMapping.entity_type == 'customer'
            ).first()
            
            if existing_mapping:
                logger.info(f"⏭️ Customer with Stripe ID {stripe_customer_id} already mapped")
                return True
            
            # Check if customer exists by email
            existing_customer = customer_crud.get_by_email(db, email)
            
            if existing_customer:
                # Update existing customer with Stripe ID and create mapping
                existing_customer.stripe_customer_id = stripe_customer_id
                
                mapping = IntegrationMapping(
                    local_entity_id=existing_customer.id,
                    integration_name='stripe',
                    external_entity_id=stripe_customer_id,
                    entity_type='customer',
                    sync_status='active',
                    last_synced_at=db.execute(text("SELECT NOW()")).scalar()
                )
                db.add(mapping)
                db.commit()
                
                logger.info(f"✅ Mapped existing customer {existing_customer.id} to Stripe {stripe_customer_id}")
                return True
            
            # Create new customer
            customer_create = CustomerCreate(name=name, email=email)
            new_customer = customer_crud.create(db, customer_create)
            
            # Set Stripe ID
            new_customer.stripe_customer_id = stripe_customer_id
            
            # Create integration mapping
            mapping = IntegrationMapping(
                local_entity_id=new_customer.id,
                integration_name='stripe',
                external_entity_id=stripe_customer_id,
                entity_type='customer',
                sync_status='active',
                last_synced_at=db.execute(text("SELECT NOW()")).scalar()
            )
            db.add(mapping)
            db.commit()
            
            logger.info(f"✅ Created customer {new_customer.id} from Stripe {stripe_customer_id}")
            print(f"📥 INBOUND CREATE: New customer {new_customer.id} from Stripe")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error handling customer create from Stripe: {e}")
            db.rollback()
            return False
    
    def _handle_customer_update(self, db: Session, payload: Dict[str, Any]) -> bool:
        """Handle customer update from Stripe."""
        try:
            stripe_customer_id = payload.get('stripe_customer_id')
            name = payload.get('name', '')
            email = payload.get('email', '')
            
            if not stripe_customer_id:
                logger.error("❌ Missing Stripe customer ID for update")
                return False
            
            # Find customer by integration mapping
            mapping = db.query(IntegrationMapping).filter(
                IntegrationMapping.external_entity_id == stripe_customer_id,
                IntegrationMapping.integration_name == 'stripe',
                IntegrationMapping.entity_type == 'customer'
            ).first()
            
            if not mapping:
                # Customer doesn't exist locally, treat as create
                logger.info(f"🔄 Customer {stripe_customer_id} not found locally, treating as create")
                return self._handle_customer_create(db, payload)
            
            # Get local customer
            customer = customer_crud.get_by_id(db, mapping.local_entity_id)
            if not customer:
                logger.error(f"❌ Local customer {mapping.local_entity_id} not found")
                return False
            
            # Check if update is needed
            if customer.name == name and customer.email == email:
                logger.debug("⏭️ Customer data unchanged, skipping update")
                # Update mapping timestamp anyway
                mapping.last_synced_at = db.execute(text("SELECT NOW()")).scalar()
                db.commit()
                return True
            
            # Update customer
            update_data = CustomerUpdate(name=name, email=email)
            updated_customer = customer_crud.update(db, customer.id, update_data)
            
            if updated_customer:
                # Update integration mapping
                mapping.last_synced_at = db.execute(text("SELECT NOW()")).scalar()
                mapping.sync_status = 'active'
                db.commit()
                
                logger.info(f"✅ Updated customer {customer.id} from Stripe {stripe_customer_id}")
                print(f"📥 INBOUND UPDATE: Customer {customer.id} updated from Stripe")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error handling customer update from Stripe: {e}")
            db.rollback()
            return False
    
    def _handle_customer_delete(self, db: Session, payload: Dict[str, Any]) -> bool:
        """Handle customer deletion from Stripe."""
        try:
            stripe_customer_id = payload.get('stripe_customer_id')
            
            if not stripe_customer_id:
                logger.error("❌ Missing Stripe customer ID for deletion")
                return False
            
            # Find customer by integration mapping
            mapping = db.query(IntegrationMapping).filter(
                IntegrationMapping.external_entity_id == stripe_customer_id,
                IntegrationMapping.integration_name == 'stripe',
                IntegrationMapping.entity_type == 'customer'
            ).first()
            
            if not mapping:
                logger.info(f"⏭️ Customer {stripe_customer_id} not found locally, nothing to delete")
                return True
            
            # Get local customer
            customer = customer_crud.get_by_id(db, mapping.local_entity_id)
            if not customer:
                logger.warning(f"⚠️ Local customer {mapping.local_entity_id} not found")
                # Update mapping status anyway
                mapping.sync_status = 'deleted'
                mapping.last_synced_at = db.execute(text("SELECT NOW()")).scalar()
                db.commit()
                return True
            
            # Delete local customer
            success = customer_crud.delete(db, customer.id)
            
            if success:
                # Update mapping status
                mapping.sync_status = 'deleted'
                mapping.last_synced_at = db.execute(text("SELECT NOW()")).scalar()
                db.commit()
                
                logger.info(f"✅ Deleted customer {customer.id} from Stripe deletion {stripe_customer_id}")
                print(f"📥 INBOUND DELETE: Customer {customer.id} deleted from Stripe")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error handling customer delete from Stripe: {e}")
            db.rollback()
            return False
    
    def handle_processing_error(self, error: Exception, message: Dict[str, Any]):
        """Handle processing errors."""
        event_id = message.get('event_id', 'unknown')
        logger.error(f"❌ Processing error for inbound event {event_id}: {error}")
        
        # Could implement dead letter queue logic here
        self.error_count += 1
    
    def get_worker_stats(self) -> Dict[str, Any]:
        """Get worker statistics."""
        return {
            "running": self.running,
            "processed_count": self.processed_count,
            "error_count": self.error_count,
            "duplicate_count": self.duplicate_count,
            "group_id": self.group_id,
            "topics": [settings.kafka.inbound_topic] if self.consumer else []
        }


# Global worker instance
inbound_worker = InboundKafkaWorker()


def start_inbound_worker():
    """Start the inbound worker (for standalone execution)."""
    inbound_worker.start_worker()


if __name__ == "__main__":
    # For running worker standalone
    logger.info("🚀 Starting standalone inbound worker...")
    start_inbound_worker()