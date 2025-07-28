"""
Stripe Webhook Server - Receives real-time events from Stripe.
Phase 5B: Webhook-based inbound sync.
"""
import json
import hmac
import hashlib
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import JSONResponse

from app.kafka_client import event_publisher
from config import settings

logger = logging.getLogger(__name__)


class StripeWebhookHandler:
    """Handles Stripe webhook events for real-time sync."""
    
    def __init__(self):
        self.webhook_secret = settings.stripe.webhook_secret if hasattr(settings.stripe, 'webhook_secret') else None
        self.supported_events = {
            'customer.created',
            'customer.updated', 
            'customer.deleted'
        }
        
    def verify_webhook_signature(self, payload: bytes, signature: str) -> bool:
        """Verify Stripe webhook signature for security."""
        if not self.webhook_secret:
            logger.warning("⚠️ No webhook secret configured, skipping signature verification")
            return True
            
        try:
            # Extract timestamp and signature from header
            elements = signature.split(',')
            timestamp = None
            signatures = []
            
            for element in elements:
                key, value = element.split('=')
                if key == 't':
                    timestamp = value
                elif key == 'v1':
                    signatures.append(value)
            
            if not timestamp or not signatures:
                logger.error("❌ Invalid signature format")
                return False
            
            # Create expected signature
            payload_for_signature = f"{timestamp}.{payload.decode('utf-8')}"
            expected_signature = hmac.new(
                self.webhook_secret.encode('utf-8'),
                payload_for_signature.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            
            # Compare signatures
            for signature in signatures:
                if hmac.compare_digest(expected_signature, signature):
                    logger.debug("✅ Webhook signature verified")
                    return True
            
            logger.error("❌ Webhook signature verification failed")
            return False
            
        except Exception as e:
            logger.error(f"❌ Error verifying webhook signature: {e}")
            return False
    
    async def handle_webhook_event(self, event_data: Dict[str, Any]) -> bool:
        """Process a Stripe webhook event."""
        try:
            event_type = event_data.get('type')
            event_id = event_data.get('id')
            
            logger.info(f"📨 Received Stripe webhook: {event_type} (ID: {event_id})")
            
            if event_type not in self.supported_events:
                logger.debug(f"⏭️ Ignoring unsupported event type: {event_type}")
                return True
            
            # Extract customer data from event
            customer_data = self._extract_customer_data(event_data)
            if not customer_data:
                logger.error(f"❌ Failed to extract customer data from event {event_id}")
                return False
            
            # Publish to Kafka for processing by inbound worker
            success = await self._publish_inbound_event(event_type, customer_data, event_id)
            
            if success:
                logger.info(f"✅ Webhook event {event_id} queued for processing")
                print(f"📨 WEBHOOK: {event_type} for customer {customer_data.get('id')} queued")
            else:
                logger.error(f"❌ Failed to queue webhook event {event_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Error handling webhook event: {e}")
            return False
    
    def _extract_customer_data(self, event_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract customer data from Stripe webhook event."""
        try:
            data_object = event_data.get('data', {}).get('object', {})
            
            if data_object.get('object') != 'customer':
                logger.warning("⚠️ Event is not for a customer object")
                return None
            
            customer_data = {
                'stripe_customer_id': data_object.get('id'),
                'name': data_object.get('name', ''),
                'email': data_object.get('email', ''),
                'metadata': data_object.get('metadata', {}),
                'created': data_object.get('created'),
                'updated': event_data.get('created'),  # Event creation time
                'deleted': data_object.get('deleted', False)
            }
            
            return customer_data
            
        except Exception as e:
            logger.error(f"❌ Error extracting customer data: {e}")
            return None
    
    async def _publish_inbound_event(self, event_type: str, customer_data: Dict[str, Any], event_id: str) -> bool:
        """Publish inbound sync event to Kafka."""
        try:
            # Map Stripe event types to our event types
            event_type_mapping = {
                'customer.created': 'INBOUND_CREATE',
                'customer.updated': 'INBOUND_UPDATE',
                'customer.deleted': 'INBOUND_DELETE'
            }
            
            mapped_event_type = event_type_mapping.get(event_type, 'INBOUND_UNKNOWN')
            
            # Prepare event payload with conflict resolution data
            event_payload = {
                'stripe_customer_id': customer_data['stripe_customer_id'],
                'name': customer_data['name'],
                'email': customer_data['email'],
                'metadata': customer_data['metadata'],
                'stripe_event_id': event_id,
                'stripe_event_type': event_type,
                'webhook_timestamp': datetime.utcnow().isoformat(),
                'source': 'stripe_webhook',
                'conflict_resolution': {
                    'timestamp': datetime.utcnow().isoformat(),
                    'source_priority': 'stripe',  # Stripe changes have priority in webhooks
                    'operation_id': f"stripe_webhook_{event_id}"
                }
            }
            
            # Use Stripe customer ID as entity_id for Kafka partitioning
            # This ensures all events for same customer go to same partition (ordering)
            entity_id_hash = abs(hash(customer_data['stripe_customer_id'])) % 1000000
            
            success = event_publisher.publish_event(
                topic=settings.kafka.inbound_topic,
                entity_type="customer",
                entity_id=entity_id_hash,
                event_type=mapped_event_type,
                payload=event_payload,
                correlation_id=f"webhook_{event_id}"
            )
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Error publishing inbound event: {e}")
            return False


# Global webhook handler
webhook_handler = StripeWebhookHandler()


# FastAPI app for webhook endpoints
webhook_app = FastAPI(
    title="Stripe Webhook Server",
    description="Receives real-time events from Stripe for inbound sync",
    version="1.0.0"
)


@webhook_app.post("/webhooks/stripe")
async def stripe_webhook_endpoint(request: Request):
    """Stripe webhook endpoint."""
    try:
        # Get raw payload and signature
        payload = await request.body()
        signature = request.headers.get('stripe-signature', '')
        
        if not signature:
            logger.error("❌ Missing Stripe signature header")
            raise HTTPException(status_code=400, detail="Missing signature")
        
        # Verify webhook signature
        if not webhook_handler.verify_webhook_signature(payload, signature):
            logger.error("❌ Invalid webhook signature")
            raise HTTPException(status_code=400, detail="Invalid signature")
        
        # Parse event data
        try:
            event_data = json.loads(payload.decode('utf-8'))
        except json.JSONDecodeError:
            logger.error("❌ Invalid JSON in webhook payload")
            raise HTTPException(status_code=400, detail="Invalid JSON")
        
        # Handle the webhook event
        success = await webhook_handler.handle_webhook_event(event_data)
        
        if success:
            return JSONResponse(
                status_code=200,
                content={"status": "success", "message": "Webhook processed"}
            )
        else:
            logger.error(f"❌ Failed to process webhook event {event_data.get('id')}")
            raise HTTPException(status_code=500, detail="Processing failed")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error in webhook endpoint: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@webhook_app.get("/webhooks/health")
async def webhook_health():
    """Health check for webhook server."""
    return {
        "status": "healthy",
        "webhook_secret_configured": webhook_handler.webhook_secret is not None,
        "supported_events": list(webhook_handler.supported_events),
        "timestamp": datetime.utcnow().isoformat()
    }


@webhook_app.get("/webhooks/test")
async def webhook_test():
    """Test endpoint to verify webhook server is accessible."""
    return {
        "message": "Webhook server is running",
        "timestamp": datetime.utcnow().isoformat(),
        "endpoint": "/webhooks/stripe"
    }

@webhook_app.post("/webhooks/test-stripe")
async def test_stripe_webhook():
    """Test endpoint that simulates a Stripe webhook without signature verification."""
    # Simulate a customer.created event
    test_event = {
        "type": "customer.created",
        "data": {
            "object": {
                "id": "cus_test_" + str(int(datetime.utcnow().timestamp())),
                "name": "Test Customer",
                "email": "test@example.com",
                "created": int(datetime.utcnow().timestamp())
            }
        }
    }
    
    try:
        # Process the event directly
        result = await webhook_handler.process_stripe_event(test_event)
        logger.info(f"✅ Test webhook processed successfully: {result}")
        
        return {
            "status": "success",
            "message": "Test webhook processed",
            "event": test_event,
            "result": result
        }
    except Exception as e:
        logger.error(f"❌ Test webhook failed: {e}")
        return {
            "status": "error",
            "message": str(e),
            "event": test_event
        }


# For running webhook server standalone
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "workers.webhook_server:webhook_app",
        host="0.0.0.0",
        port=8001,  # Different port from main API
        reload=True
    )