"""
Minimal Settings - Simple and works with your .env file.
"""
import os
from typing import Optional, Any

# Load environment variables from .env if it exists
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class SimpleSettings:
    """Simple settings class that just reads from environment."""
    
    def __init__(self):
        # Application
        self.app_name = "Customer Integration API"
        self.app_version = "2.0.0"
        self.debug = os.getenv("DEBUG", "false").lower() == "true"
        
        # API
        self.api_host = os.getenv("API_HOST", "0.0.0.0")
        self.api_port = int(os.getenv("API_PORT", "8000"))
        
        # Database
        self.database = DatabaseConfig()
        
        # Kafka
        self.kafka = KafkaConfig()
        
        # Stripe
        self.stripe = StripeConfig()
        
        # Workers
        self.workers = WorkerConfig()
        
        # Features
        self.enable_outbound_sync = os.getenv("ENABLE_OUTBOUND_SYNC", "true").lower() == "true"
        self.enable_inbound_sync = os.getenv("ENABLE_INBOUND_SYNC", "true").lower() == "true"
        self.enable_webhooks = os.getenv("ENABLE_WEBHOOKS", "true").lower() == "true"
        self.enable_kafka_workers = os.getenv("ENABLE_KAFKA_WORKERS", "true").lower() == "true"
        self.enable_inbound_polling = os.getenv("ENABLE_INBOUND_POLLING", "false").lower() == "true"


class DatabaseConfig:
    """Database configuration."""
    
    def __init__(self):
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.port = int(os.getenv("POSTGRES_PORT", "5432"))
        self.user = os.getenv("POSTGRES_USER", "user")
        self.password = os.getenv("POSTGRES_PASSWORD", "password")
        self.database = os.getenv("POSTGRES_DB", "zenskar_customer_integration")
    
    @property
    def url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


class KafkaConfig:
    """Kafka configuration."""
    
    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.outbound_topic = os.getenv("KAFKA_OUTBOUND_TOPIC", "customer_outbound_events")
        self.inbound_topic = os.getenv("KAFKA_INBOUND_TOPIC", "customer_inbound_events")
        self.group_id = os.getenv("KAFKA_GROUP_ID", "customer_integration_group")
        self.outbound_worker_group = os.getenv("KAFKA_OUTBOUND_WORKER_GROUP", "outbound_sync_group")
        self.inbound_worker_group = os.getenv("KAFKA_INBOUND_WORKER_GROUP", "inbound_sync_group")


class StripeConfig:
    """Stripe configuration."""
    
    def __init__(self):
        self.secret_key = os.getenv("STRIPE_SECRET_KEY")
        self.webhook_secret = os.getenv("STRIPE_WEBHOOK_SECRET")
        self.api_version = os.getenv("STRIPE_API_VERSION", "2023-10-16")
        self.webhook_url = os.getenv("STRIPE_WEBHOOK_URL")
        self.webhook_events = [
           "customer.created",
           "customer.updated", 
           "customer.deleted"
        ]


class WorkerConfig:
    """Worker configuration."""
    
    def __init__(self):
        self.webhook_enabled = os.getenv("WEBHOOK_ENABLED", "true").lower() == "true"
        self.webhook_host = os.getenv("WEBHOOK_HOST", "0.0.0.0")
        self.webhook_port = int(os.getenv("WEBHOOK_PORT", "8001"))
        self.outbound_worker_enabled = os.getenv("OUTBOUND_WORKER_ENABLED", "true").lower() == "true"
        self.inbound_worker_enabled = os.getenv("INBOUND_WORKER_ENABLED", "true").lower() == "true"


# Global settings instance
settings = SimpleSettings()

# Print config for debugging
if settings.debug:
    print(f"🔧 Settings loaded:")
    print(f"  Database: {settings.database.host}:{settings.database.port}")
    print(f"  Kafka: {settings.kafka.bootstrap_servers}")
    print(f"  Stripe configured: {bool(settings.stripe.secret_key)}")
    print(f"  Webhook URL: {settings.stripe.webhook_url}")