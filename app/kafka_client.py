"""
Real Kafka client with better error handling and fallback.
"""
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Callable
import threading
import time
from config import settings

logger = logging.getLogger(__name__)

# Try to import real Kafka, fallback to mock if it fails
try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.errors import KafkaError, KafkaTimeoutError
    KAFKA_AVAILABLE = True
    print("✅ Real Kafka libraries imported successfully")
except ImportError as e:
    print(f"⚠️ Kafka import failed: {e}")
    print("🎭 Falling back to Mock Kafka for testing")
    KAFKA_AVAILABLE = False


class KafkaEventPublisher:
    """Kafka event publisher with fallback to mock."""

    def __init__(self):
        self.producer = None
        self.connected = False
        self.use_mock = not KAFKA_AVAILABLE
        self._connect()

    def _connect(self):
        """Initialize Kafka producer with retry logic."""
        if not KAFKA_AVAILABLE:
            self._setup_mock_producer()
            return
            
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries and not self.connected:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=settings.kafka.bootstrap_servers.split(','),
                    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                    key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                    acks='all',
                    retries=3,
                    retry_backoff_ms=1000,
                    request_timeout_ms=30000,
                    compression_type='gzip',
                    batch_size=16384,
                    linger_ms=10,
                    api_version=(0, 10, 1),
                    api_version_auto_timeout_ms=10000
                )
                self.connected = True
                self.use_mock = False
                logger.info("✅ Real Kafka producer connected")
                print("🔗 REAL KAFKA: Producer connected to", settings.kafka.bootstrap_servers)
                
            except Exception as e:
                retry_count += 1
                logger.error(f"❌ Kafka connection attempt {retry_count} failed: {e}")
                if retry_count < max_retries:
                    time.sleep(2 ** retry_count)
                else:
                    logger.error("❌ Failed to connect to real Kafka, using mock")
                    self._setup_mock_producer()
    
    def _setup_mock_producer(self):
        """Setup mock producer as fallback."""
        self.use_mock = True
        self.connected = True
        logger.info("🎭 Mock Kafka producer initialized as fallback")
        print("🎭 MOCK KAFKA: Producer initialized (fallback mode)")

    def publish_event(self, 
                     topic: str,
                     entity_type: str,
                     entity_id: int,
                     event_type: str,
                     payload: Dict[str, Any],
                     correlation_id: Optional[str] = None) -> bool:
        """Publish an event to Kafka."""
        if not self.connected:
            logger.error("❌ Kafka producer not connected")
            return False

        event = {
            "event_id": f"{entity_type}_{entity_id}_{event_type}_{int(time.time())}",
            "entity_type": entity_type,
            "entity_id": entity_id,
            "event_type": event_type,
            "source_system": "LOCAL",
            "payload": payload,
            "timestamp": datetime.utcnow().isoformat(),
            "correlation_id": correlation_id or f"corr_{int(time.time())}"
        }

        try:
            if self.use_mock:
                return self._mock_publish(topic, event)
            else:
                return self._real_publish(topic, event, entity_id)
                
        except Exception as e:
            logger.error(f"❌ Error publishing event {event['event_id']}: {e}")
            return False
    
    def _real_publish(self, topic: str, event: Dict, entity_id: int) -> bool:
        """Publish to real Kafka."""
        future = self.producer.send(
            topic=topic,
            key=str(entity_id),
            value=event
        )
        
        record_metadata = future.get(timeout=30)
        
        logger.info(
            f"✅ REAL KAFKA: Event published: {event['event_id']} to "
            f"{record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}"
        )
        print(f"📤 REAL KAFKA: {event['event_type']} → {topic}")
        return True
    
    def _mock_publish(self, topic: str, event: Dict) -> bool:
        """Mock publish for fallback."""
        logger.info(f"🎭 MOCK: Event published: {event['event_id']} to {topic}")
        print(f"📤 MOCK KAFKA: {event['event_type']} → {topic} (fallback)")
        return True

    def close(self):
        """Close the Kafka producer."""
        if self.producer and not self.use_mock:
            self.producer.flush()
            self.producer.close()
        self.connected = False
        logger.info("✅ Kafka producer closed")


class KafkaEventConsumer:
    """Kafka event consumer with fallback to mock."""

    def __init__(self, topics: list, group_id: str = None, auto_offset_reset: str = 'latest'):
        self.topics = topics
        self.group_id = group_id or settings.kafka.group_id
        self.auto_offset_reset = auto_offset_reset
        self.consumer = None
        self.running = False
        self.consumer_thread = None
        self.use_mock = not KAFKA_AVAILABLE
        
    def _create_consumer(self):
        """Create Kafka consumer."""
        if not KAFKA_AVAILABLE:
            return self._create_mock_consumer()
            
        try:
            return KafkaConsumer(
                *self.topics,
                bootstrap_servers=settings.kafka.bootstrap_servers.split(','),
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                auto_offset_reset=self.auto_offset_reset,
                enable_auto_commit=False,
                max_poll_records=100,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                max_poll_interval_ms=300000,
                consumer_timeout_ms=1000,
                api_version=(0, 10, 1),
                api_version_auto_timeout_ms=10000
            )
        except Exception as e:
            logger.error(f"❌ Failed to create real Kafka consumer: {e}")
            return self._create_mock_consumer()
    
    def _create_mock_consumer(self):
        """Create mock consumer."""
        self.use_mock = True
        return MockKafkaConsumer(self.topics, self.group_id)

    def start_consuming(self, message_handler: Callable[[Dict[str, Any]], bool], 
                       error_handler: Optional[Callable[[Exception, Dict[str, Any]], None]] = None):
        """Start consuming messages in a separate thread."""
        if self.running:
            logger.warning("Consumer already running")
            return

        def consume_loop():
            try:
                self.consumer = self._create_consumer()
                self.running = True
                
                if self.use_mock:
                    logger.info(f"🎭 MOCK: Started consuming from topics: {self.topics}")
                    print(f"📥 MOCK CONSUMER: Listening on {self.topics}")
                    # Mock consumer just runs and simulates activity
                    while self.running:
                        time.sleep(5)
                        logger.debug("🎭 Mock consumer running...")
                else:
                    logger.info(f"✅ REAL KAFKA: Started consuming from topics: {self.topics}")
                    print(f"📥 REAL CONSUMER: Listening on {self.topics}")
                    
                    while self.running:
                        try:
                            message_batch = self.consumer.poll(timeout_ms=1000)
                            
                            for topic_partition, messages in message_batch.items():
                                for message in messages:
                                    try:
                                        print(f"📥 REAL KAFKA: Processing message from {topic_partition}")
                                        success = message_handler(message.value)
                                        
                                        if success:
                                            self.consumer.commit_async()
                                            logger.debug(f"✅ Processed message: {message.value.get('event_id', 'unknown')}")
                                        else:
                                            logger.error(f"❌ Failed to process message: {message.value.get('event_id', 'unknown')}")
                                            
                                    except Exception as e:
                                        logger.error(f"❌ Error processing message: {e}")
                                        if error_handler:
                                            error_handler(e, message.value)
                                            
                        except Exception as e:
                            if self.running:
                                logger.error(f"❌ Error in consumer loop: {e}")
                                time.sleep(5)
                            
            except Exception as e:
                logger.error(f"❌ Fatal error in consumer: {e}")
            finally:
                if self.consumer and hasattr(self.consumer, 'close'):
                    self.consumer.close()
                status = "REAL" if not self.use_mock else "MOCK"
                logger.info(f"✅ {status} Consumer stopped")

        self.consumer_thread = threading.Thread(target=consume_loop, daemon=True)
        self.consumer_thread.start()

    def stop_consuming(self):
        """Stop consuming messages."""
        if self.running:
            self.running = False
            if self.consumer_thread:
                self.consumer_thread.join(timeout=10)
            logger.info("✅ Consumer shutdown complete")


class MockKafkaConsumer:
    """Simple mock consumer for fallback."""
    
    def __init__(self, topics, group_id):
        self.topics = topics
        self.group_id = group_id
    
    def close(self):
        pass


# Global instances
event_publisher = KafkaEventPublisher()


def create_consumer(topics: list, group_id: str = None) -> KafkaEventConsumer:
    """Create a new consumer."""
    return KafkaEventConsumer(topics, group_id)