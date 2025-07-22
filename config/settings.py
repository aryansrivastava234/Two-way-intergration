import os
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


@dataclass
class DatabaseConfig:
    """Database configuration."""
    user: str = os.getenv("POSTGRES_USER", "user")
    password: str = os.getenv("POSTGRES_PASSWORD", "password")
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    database: str = os.getenv("POSTGRES_DB", "zenskar_customer_integration")
    
    @property
    def url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class KafkaConfig:
    """Kafka configuration."""
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    group_id: str = os.getenv("KAFKA_GROUP_ID", "customer-integration")
    
    # Topics
    outbound_topic: str = "customer-outbound-events"
    inbound_topic: str = "customer-inbound-events"
    dead_letter_topic: str = "customer-dead-letter"


@dataclass
class StripeConfig:
    """Stripe API configuration."""
    secret_key: str = os.getenv("STRIPE_SECRET_KEY", "")
    webhook_secret: str = os.getenv("STRIPE_WEBHOOK_SECRET", "")
    api_version: str = "2023-10-16"


@dataclass
class AppConfig:
    """Main application configuration."""
    debug: bool = os.getenv("DEBUG", "false").lower() == "true"
    api_host: str = os.getenv("API_HOST", "0.0.0.0")
    api_port: int = int(os.getenv("API_PORT", "8000"))
    webhook_host: str = os.getenv("WEBHOOK_HOST", "localhost")
    webhook_port: int = int(os.getenv("WEBHOOK_PORT", "8001"))


# Create global instances
database = DatabaseConfig()
kafka = KafkaConfig()
stripe = StripeConfig()
app = AppConfig()

# For backward compatibility, create a settings object
class Settings:
    def __init__(self):
        self.database = database
        self.kafka = kafka
        self.stripe = stripe
        self.debug = app.debug
        self.api_host = app.api_host
        self.api_port = app.api_port
        self.webhook_host = app.webhook_host
        self.webhook_port = app.webhook_port

settings = Settings()