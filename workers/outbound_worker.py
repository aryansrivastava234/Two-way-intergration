"""
Outbound Kafka Worker - Processes local changes and syncs to Stripe.
Phase 6: Real Kafka workers with conflict resolution.
"""
import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import SessionLocal
from app.kafka_client import create_consumer
from app.services.sync_service import outbound_sync_service
from app.crud.customer import customer_crud
from app.models.customer import Customer
from config import settings

logger = logging.getLogger(__name__)


class OutboundKafkaWorker:
    """Processes outbound sync events from Kafka queue."""
    
    def __init__(self, group_id: str = "outbound_sync_group"):
        self.group_id = group_id
        self.consumer = None
        self.running = False
        self.processed_count = 0
        self.error_count = 0
        
    def start_worker(self):
        """Start the outbound worker."""
        logger.info("🚀 Starting outbound Kafka worker...")
        
        # Create Kafka consumer
        topics = [settings.kafka.outbound_topic]
        self.consumer = create_consumer(topics, self.group_id)
        
        # Start consuming messages
        self.consumer.start_consuming(
            message_handler=self.process_outbound_message,
            error_handler=self.handle_processing_error
        )
        
        self.running = True
        logger.info(f"✅ Outbound worker started, consuming from: {topics}")
        print(f"🔄 OUTBOUND WORKER: Listening for local → Stripe sync events")
        
        # Keep worker running
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("⏸️ Received stop signal")
        finally:
            self.stop_worker()
    
    def stop_worker(self):
        """Stop the outbound worker."""
        logger.info("🛑 Stopping outbound worker...")
        self.running = False
        
        if self.consumer:
            self.consumer.stop_consuming()
        
        logger.info(f"✅ Outbound worker stopped. Processed: {self.processed_count}, Errors: {self.error_count}")
    
    def process_outbound_message(self, message: Dict[str, Any]) -> bool:
        """Process a single outbound sync message."""
        try:
            event_id = message.get('event_id', 'unknown')
            entity_type = message.get('entity_type')
            entity_id = message.get('entity_id')
            event_type = message.get('event_type')
            payload = message.get('payload', {})
            
            logger.info(f"📤 Processing outbound event: {event_id} ({event_type})")
            print(f"📤 OUTBOUND: Processing {event_type} for customer {entity_id}")
            
            # Validate message structure
            if not all([entity_type, entity_id, event_type]):
                logger.error(f"❌ Invalid message structure: {event_id}")
                return False
            
            # Only process customer events
            if entity_type != 'customer':
                logger.debug(f"⏭️ Ignoring non-customer event: {entity_type}")
                return True
            
            # Check for conflict resolution data
            conflict_info = payload.get('conflict_resolution', {})
            if conflict_info and not self._should_process_event(conflict_info):
                logger.info(f"⏭️ Skipping event due to conflict resolution: {event_id}")
                return True
            
            # Process based on event type
            success = False
            db = SessionLocal()
            try:
                if event_type == 'CREATE':
                    success = self._handle_customer_create(db, entity_id, payload)
                elif event_type == 'UPDATE':
                    success = self._handle_customer_update(db, entity_id, payload)
                elif event_type == 'DELETE':
                    success = self._handle_customer_delete(db, entity_id, payload)
                else:
                    logger.warning(f"⚠️ Unknown event type: {event_type}")
                    return True  # Don't retry unknown events
                
            finally:
                db.close()
            
            if success:
                self.processed_count += 1
                logger.info(f"✅ Successfully processed outbound event: {event_id}")
                print(f"✅ OUTBOUND: {event_type} completed for customer {entity_id}")
            else:
                self.error_count += 1
                logger.error(f"❌ Failed to process outbound event: {event_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Error processing outbound message: {e}")
            self.error_count += 1
            return False
    
    def _should_process_event(self, conflict_info: Dict[str, Any]) -> bool:
        """Determine if event should be processed based on conflict resolution."""
        try:
            source_priority = conflict_info.get('source_priority', 'local')
            timestamp_str = conflict_info.get('timestamp')
            
            if not timestamp_str:
                return True  # No timestamp, process normally
            
            # For outbound events, we generally want to process local changes
            # unless they're too old (> 5 minutes)
            event_timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            age_seconds = (datetime.utcnow() - event_timestamp.replace(tzinfo=None)).total_seconds()
            
            if age_seconds > 300:  # 5 minutes
                logger.warning(f"⚠️ Event is too old ({age_seconds}s), skipping")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error in conflict resolution: {e}")
            return True  # Process on error
    
    def _handle_customer_create(self, db: Session, customer_id: int, payload: Dict[str, Any]) -> bool:
        """Handle customer creation event."""
        try:
            customer = customer_crud.get_by_id(db, customer_id)
            if not customer:
                logger.error(f"❌ Customer {customer_id} not found for CREATE event")
                return False
            
            # Skip if already synced to Stripe
            if customer.stripe_customer_id:
                logger.info(f"⏭️ Customer {customer_id} already has Stripe ID, skipping create")
                return True
            
            success = outbound_sync_service.sync_customer_create(db, customer)
            return success
            
        except Exception as e:
            logger.error(f"❌ Error handling customer create: {e}")
            return False
    
    def _handle_customer_update(self, db: Session, customer_id: int, payload: Dict[str, Any]) -> bool:
        """Handle customer update event."""
        try:
            customer = customer_crud.get_by_id(db, customer_id)
            if not customer:
                logger.error(f"❌ Customer {customer_id} not found for UPDATE event")
                return False
            
            success = outbound_sync_service.sync_customer_update(db, customer)
            return success
            
        except Exception as e:
            logger.error(f"❌ Error handling customer update: {e}")
            return False
    
    def _handle_customer_delete(self, db: Session, customer_id: int, payload: Dict[str, Any]) -> bool:
        """Handle customer deletion event."""
        try:
            # For delete events, customer might already be deleted from local DB
            # Use data from payload
            stripe_customer_id = payload.get('stripe_customer_id')
            
            if not stripe_customer_id:
                logger.warning(f"⚠️ No Stripe ID in delete payload for customer {customer_id}")
                return True  # Nothing to delete in Stripe
            
            # Delete from Stripe using the ID from payload
            from app.integrations import integration_manager
            stripe_integration = integration_manager.get_integration('stripe')
            
            if stripe_integration:
                result = stripe_integration.delete_customer(stripe_customer_id)
                if result.success:
                    logger.info(f"✅ Deleted customer {stripe_customer_id} from Stripe")
                    return True
                else:
                    logger.error(f"❌ Failed to delete customer from Stripe: {result.error_message}")
                    return False
            else:
                logger.error("❌ No Stripe integration available")
                return False
            
        except Exception as e:
            logger.error(f"❌ Error handling customer delete: {e}")
            return False
    
    def handle_processing_error(self, error: Exception, message: Dict[str, Any]):
        """Handle processing errors."""
        event_id = message.get('event_id', 'unknown')
        logger.error(f"❌ Processing error for event {event_id}: {error}")
        
        # Could implement dead letter queue logic here
        # For now, just log and continue
        self.error_count += 1
    
    def get_worker_stats(self) -> Dict[str, Any]:
        """Get worker statistics."""
        return {
            "running": self.running,
            "processed_count": self.processed_count,
            "error_count": self.error_count,
            "group_id": self.group_id,
            "topics": [settings.kafka.outbound_topic] if self.consumer else []
        }


# Global worker instance
outbound_worker = OutboundKafkaWorker()


def start_outbound_worker():
    """Start the outbound worker (for standalone execution)."""
    outbound_worker.start_worker()


if __name__ == "__main__":
    # For running worker standalone
    logger.info("🚀 Starting standalone outbound worker...")
    start_outbound_worker()