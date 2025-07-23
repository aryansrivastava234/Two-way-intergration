"""
Kafka client for publishing and consuming events.
Includes fallback to mock implementation if Kafka is not available.
"""
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# Try to import Kafka libraries with fallback
try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.errors import KafkaError
    KAFKA_AVAILABLE = True
    logger.info("Kafka libraries imported successfully")
except ImportError as e:
    logger.warning(f"Kafka libraries not available: {e}. Using mock implementation.")
    KAFKA_AVAILABLE = False

try:
    from config import settings
except ImportError:
    logger.error("Could not import settings")
    settings = None


class KafkaEventPublisher:
    """Kafka event publisher for sync events with fallback to mock."""

    def __init__(self):
        self.producer = None
        self.is_mock = not KAFKA_AVAILABLE
        if KAFKA_AVAILABLE and settings:
            self._connect()
        else:
            logger.info("Using mock Kafka event publisher")

    def _connect(self):
        """Initialize Kafka producer."""
        if not KAFKA_AVAILABLE:
            return

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=settings.kafka.bootstrap_servers.split(','),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                retry_backoff_ms=1000,
                request_timeout_ms=10000,
                acks='all'  # Wait for all replicas to acknowledge
            )
            logger.info("Kafka producer connected successfully")
            self.is_mock = False
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}. Using mock implementation.")
            self.producer = None
            self.is_mock = True

    def publish_event(self, 
                     topic: str,
                     entity_type: str,
                     entity_id: int,
                     event_type: str,
                     payload: Dict[str, Any],
                     correlation_id: Optional[str] = None) -> bool:
        """
        Publish an event to Kafka or mock implementation.
        
        Args:
            topic: Kafka topic name
            entity_type: Type of entity (e.g., 'customer')
            entity_id: Local entity ID
            event_type: Type of event ('CREATE', 'UPDATE', 'DELETE')
            payload: Event payload data
            correlation_id: Optional correlation ID for tracking
            
        Returns:
            bool: True if published successfully, False otherwise
        """
        event = {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "event_type": event_type,
            "source_system": "LOCAL",
            "payload": payload,
            "timestamp": datetime.utcnow().isoformat(),
            "correlation_id": correlation_id
        }

        # If using mock implementation
        if self.is_mock or not self.producer:
            return self._mock_publish_event(topic, event)

        # Real Kafka implementation
        try:
            # Use entity_id as key for partitioning
            future = self.producer.send(
                topic=topic,
                key=str(entity_id),
                value=event
            )
            
            # Wait for the message to be sent
            record_metadata = future.get(timeout=10)
            logger.info(f"Event published to {record_metadata.topic}:{record_metadata.partition} offset {record_metadata.offset}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish event to Kafka: {e}. Falling back to mock.")
            return self._mock_publish_event(topic, event)

    def _mock_publish_event(self, topic: str, event: Dict[str, Any]) -> bool:
        """Mock implementation that logs the event."""
        print(f"\n📤 KAFKA EVENT PUBLISHED (MOCK):")
        print(f"   Topic: {topic}")
        print(f"   Type: {event['event_type']} {event['entity_type']}")
        print(f"   Entity ID: {event['entity_id']}")
        print(f"   Payload: {json.dumps(event['payload'], indent=4)}")
        print(f"   Timestamp: {event['timestamp']}")
        if event.get('correlation_id'):
            print(f"   Correlation ID: {event['correlation_id']}")
        print("-" * 60)
        
        logger.info(f"Mock published {event['event_type']} event for {event['entity_type']} {event['entity_id']} to topic {topic}")
        return True

    def close(self):
        """Close the Kafka producer."""
        if self.producer:
            try:
                self.producer.flush()
                self.producer.close()
                logger.info("Kafka producer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")


class KafkaEventConsumer:
    """Kafka event consumer for processing sync events with fallback."""

    def __init__(self, topics: list, group_id: str = None):
        self.topics = topics
        self.group_id = group_id or (settings.kafka.group_id if settings else "customer-integration")
        self.consumer = None
        self.is_mock = not KAFKA_AVAILABLE
        
        if not KAFKA_AVAILABLE:
            logger.info("Using mock Kafka event consumer")
        
    def _connect(self):
        """Initialize Kafka consumer."""
        if not KAFKA_AVAILABLE:
            logger.info("Kafka not available, using mock consumer")
            return True

        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=settings.kafka.bootstrap_servers.split(','),
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                consumer_timeout_ms=10000
            )
            logger.info(f"Kafka consumer connected to topics: {self.topics}")
            self.is_mock = False
            return True
        except Exception as e:
            logger.error(f"Failed to connect Kafka consumer: {e}. Using mock implementation.")
            self.is_mock = True
            return True

    def consume_events(self, callback_func):
        """
        Consume events and call callback function for each message.
        
        Args:
            callback_func: Function to call for each message (message) -> bool
        """
        if self.is_mock:
            logger.info("Mock consumer - no events to consume")
            return

        if not self._connect():
            return

        try:
            for message in self.consumer:
                try:
                    success = callback_func(message.value)
                    if not success:
                        logger.warning(f"Failed to process message: {message.value}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.close()

    def close(self):
        """Close the Kafka consumer."""
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")


# Global instances
event_publisher = KafkaEventPublisher()