# Zenskar Customer Integration System

A comprehensive customer data synchronization system that provides bidirectional sync between your application and Stripe, with real-time webhook support and conflict resolution.

## 🎯 Requirements Implementation

This project implements all the specified requirements:

### ✅ **Customer Table with SQL Database**
- **PostgreSQL** database with customer table containing (ID, name, email)
- ID is internal to our system, not related to external identifiers
- Includes additional fields for integration mapping and timestamps
- Located: `app/models/customer.py`

### ✅ **Stripe Integration**
- Free Stripe test account integration
- Complete CRUD operations with Stripe API
- Located: `app/integrations/stripe_integration.py`

### ✅ **Kafka Queue System**
- Docker-based Kafka setup with Zookeeper
- Used as event queue for sync operations
- Fallback to mock implementation for development
- Located: `docker-compose.yml`, `app/kafka_client.py`

### ✅ **Outbound Sync (Local → Stripe)**
- Changes to local customers automatically sync to Stripe
- Events queued via Kafka for reliable processing
- Worker processes events and updates Stripe
- Located: `workers/outbound_worker.py`

### ✅ **Inbound Sync (Stripe → Local)**
- Webhook API server receives Stripe events
- Events processed via Kafka queue
- Real-time updates from Stripe to local database
- Located: `workers/webhook_server.py`, `workers/inbound_worker.py`

### ✅ **Extensible Architecture**
- Base integration classes for easy extension
- Integration manager for multiple systems
- Located: `app/integrations/base.py`, `app/integrations/manager.py`

## Features

- **Bidirectional Sync**: Keep customer data synchronized between your database and Stripe
- **Real-time Webhooks**: Process Stripe events in real-time via webhooks
- **Conflict Resolution**: Intelligent handling of concurrent updates with first-write-wins
- **Event-driven Architecture**: Kafka-based event processing with fallback support
- **REST API**: Complete CRUD operations for customers
- **Database Integration**: PostgreSQL with SQLAlchemy ORM
- **Extensible Design**: Easy to add new integrations (Salesforce, etc.)

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   FastAPI       │    │     Kafka        │    │   Stripe API    │
│   REST API      │◄──►│   Event Bus      │◄──►│   Integration   │
│   (Port 8000)   │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   PostgreSQL    │    │    Workers       │    │   Webhook       │
│   Database      │    │  • Inbound       │    │   Server        │
│                 │    │  • Outbound      │    │  (Port 8001)    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Quick Start

### Prerequisites

- Python 3.8+
- Docker & Docker Compose (for PostgreSQL and Kafka)
- Stripe CLI (optional, for webhook testing)

### Installation & Setup

1. **Clone and setup environment:**
   ```bash
   cd Zenskar_Assignment
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Configure environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your Stripe keys and database credentials
   ```

3. **Start the application:**
   ```bash
   python startup.py
   ```

**What the startup script does:**
- ✅ Checks all dependencies (Docker, docker-compose)
- 🐳 Starts PostgreSQL and Kafka infrastructure
- 🗄️ Runs database migrations (if needed)
- 🌐 Launches the FastAPI server (http://localhost:8000)
- ⚡ Starts all background workers and webhook handlers
- 📊 Displays access information and monitoring URLs

The startup script provides comprehensive logging and graceful shutdown with `Ctrl+C`.

### Manual Service Management (Alternative)

If you prefer to start services individually:

1. **Start infrastructure:**
   ```bash
   docker-compose up -d  # PostgreSQL and Kafka only
   ```

2. **Start API server:**
   ```bash
   python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
   # API available at: http://localhost:8000
   ```

3. **Start workers:**
   ```bash
   python -m workers.worker_manager
   ```

### Stripe Integration Setup

**Configure webhook endpoint in Stripe dashboard:**
- Webhook URL: `http://localhost:8001/webhooks/stripe`
- Events to send: `customer.created`, `customer.updated`, `customer.deleted`

**Or use Stripe CLI for testing:**
```bash
stripe login
stripe listen --forward-to localhost:8001/webhooks/stripe
```

## API Endpoints

### Customer Management
- `GET /customers` - List all customers
- `POST /customers` - Create a new customer
- `GET /customers/{id}` - Get customer by ID
- `PUT /customers/{id}` - Update customer
- `DELETE /customers/{id}` - Delete customer

### Sync Operations
- `POST /customers/{id}/sync` - Manually sync customer to Stripe
- `GET /sync/status` - Get sync status
- `GET /health` - Health check

### Example Usage

```bash
# Create a customer (automatically syncs to Stripe)
curl -X POST "http://localhost:8000/customers" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "John Doe",
    "email": "john@example.com"
  }'

# Sync customer to Stripe manually
curl -X POST "http://localhost:8000/customers/1/sync"

# Get sync status
curl "http://localhost:8000/sync/status"
```

## Configuration

### Environment Variables

```bash
# Database
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=user
POSTGRES_PASSWORD=password
POSTGRES_DB=zenskar_customer_integration

# Stripe
STRIPE_SECRET_KEY=sk_test_...
STRIPE_WEBHOOK_SECRET=whsec_...

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_OUTBOUND_TOPIC=customer_outbound_events
KAFKA_INBOUND_TOPIC=customer_inbound_events

# Application
API_HOST=0.0.0.0
API_PORT=8000
WEBHOOK_HOST=localhost
WEBHOOK_PORT=8001
```

## Database Schema

### Tables

- **`customers`** - Local customer records (ID, name, email, stripe_customer_id, timestamps)
- **`integration_mappings`** - Maps local customers to external system IDs
- **`sync_events`** - Audit log of all sync operations
- **`processed_events`** - Kafka event deduplication
- **`processed_stripe_events`** - Stripe webhook event deduplication

## Event Flow

### Outbound Sync (Local → Stripe)
1. Customer created/updated via API
2. Event published to `customer_outbound_events` topic
3. Outbound worker processes event
4. Stripe API call made
5. Integration mapping updated

### Inbound Sync (Stripe → Local)
1. Stripe webhook received at `/webhooks/stripe`
2. Event published to `customer_inbound_events` topic
3. Inbound worker processes event
4. Local database updated
5. Integration mapping updated

## 🔌 Extensibility Plans

### Adding Salesforce Integration

The current architecture is designed for easy extension to support additional integrations:

**1. Implementation Steps:**
```python
# Create new integration class
class SalesforceIntegration(BaseIntegration):
    def create_customer(self, customer_data: CustomerData) -> SyncResult:
        # Salesforce API calls using simple_salesforce library
        pass
    
    def update_customer(self, external_id: str, customer_data: CustomerData) -> SyncResult:
        # Update Salesforce contact
        pass
    
    # Implement other required methods...
```

**2. Configuration Updates:**
- Add Salesforce credentials to environment variables
- Register integration in `IntegrationManager`
- Update database schema to support multiple external IDs per customer

**3. Worker Extensions:**
- Modify workers to handle multiple integration targets
- Add routing logic based on integration preferences
- Implement priority-based sync (e.g., Stripe first, then Salesforce)

**4. Conflict Resolution:**
- Extend conflict resolution to handle 3-way sync
- Implement integration priority rules
- Add timestamp-based conflict resolution

### Supporting Other Entity Types (Invoice Catalog)

The system can be extended to support other business entities:

**1. Entity-Agnostic Architecture:**
```python
# Generic base models
class BaseEntity(Base):
    __abstract__ = True
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

class Invoice(BaseEntity):
    __tablename__ = "invoices"
    customer_id = Column(Integer, ForeignKey('customers.id'))
    amount = Column(Decimal)
    status = Column(String)
    # Stripe invoice ID, Salesforce opportunity ID, etc.
```

**2. Generic Integration Framework:**
```python
class BaseEntityIntegration(ABC):
    @abstractmethod
    def sync_entity(self, entity_type: str, entity_data: Dict) -> SyncResult:
        pass

class StripeInvoiceIntegration(BaseEntityIntegration):
    def sync_entity(self, entity_type: str, entity_data: Dict) -> SyncResult:
        if entity_type == 'invoice':
            return self.sync_invoice(entity_data)
        elif entity_type == 'customer':
            return self.sync_customer(entity_data)
```

**3. Event System Extensions:**
- Use entity_type field in Kafka events
- Route events to appropriate workers based on entity type
- Maintain separate topics per entity type for better scalability

**4. API Endpoints:**
- Generic CRUD endpoints: `/entities/{entity_type}`
- Entity-specific endpoints: `/invoices`, `/products`, etc.
- Cross-entity operations: `/customers/{id}/invoices`

## Monitoring

### Health Checks
- API: `GET http://localhost:8000/health`
- Webhook: `GET http://localhost:8001/webhooks/health`

### Logs
- `startup.log` - System startup and management logs
- `api.log` - API server logs
- `inbound_worker.log` - Inbound sync worker logs
- `outbound_worker.log` - Outbound sync worker logs
- `webhook_server.log` - Webhook server logs

### Database Monitoring
```sql
-- Check integration mappings
SELECT * FROM integration_mappings;

-- Check recent sync events
SELECT * FROM sync_events ORDER BY created_at DESC LIMIT 10;

-- Check processed events
SELECT * FROM processed_events ORDER BY created_at DESC LIMIT 10;
```

## Testing

### Unit Tests
```bash
python -m pytest tests/unit/
```

### Integration Tests
```bash
python -m pytest tests/integration/
```

### Manual Testing
```bash
# Test webhook endpoint
curl http://localhost:8001/webhooks/test

# Test Stripe integration
python tests/integration/test_stripe_integration.py
```

## Development

### Project Structure
```
├── app/                    # Core application
│   ├── api/               # FastAPI routes
│   ├── crud/              # Database operations
│   ├── integrations/      # External service integrations
│   │   ├── base.py       # Base integration classes
│   │   ├── manager.py    # Integration manager
│   │   └── stripe_integration.py
│   ├── models/            # SQLAlchemy models
│   ├── services/          # Business logic
│   ├── database.py        # Database configuration
│   ├── kafka_client.py    # Kafka client with fallback
│   └── main.py           # FastAPI application
├── config/                # Configuration
├── migrations/            # Database migrations
├── tests/                 # Test suite
├── workers/               # Background workers
│   ├── inbound_worker.py  # Stripe → Local sync
│   ├── outbound_worker.py # Local → Stripe sync
│   ├── webhook_server.py  # Stripe webhook handler
│   └── worker_manager.py  # Worker orchestration
├── docker-compose.yml     # Infrastructure setup
├── startup.py            # Professional startup script
└── requirements.txt      # Python dependencies
```

## Troubleshooting

### Common Issues

1. **Stripe CLI not working**: Ensure `/usr/local/bin/stripe` is the binary, not a directory
2. **Webhook 404 errors**: Check endpoint URL is `/webhooks/stripe` (with 's')
3. **Database connection errors**: Verify PostgreSQL is running and credentials are correct
4. **Kafka connection errors**: Ensure Kafka is running via `docker-compose up kafka`

### Debug Commands
```bash
# Check service status
docker-compose ps

# View logs
docker-compose logs postgres
docker-compose logs kafka

# Test database connection
python -c "from app.database import engine; print('Database connected!')"

# Test Kafka connection
python tests/integration/test_kafka.py
```

## Project Highlights

### ✨ **Professional Implementation**
- Comprehensive error handling and logging
- Graceful shutdown and resource management
- Production-ready configuration management
- Professional startup script with dependency checking

### 🔄 **Robust Sync Architecture**
- Bidirectional sync with conflict resolution
- Deduplication for both Kafka and Stripe events
- Retry logic and error handling
- Integration mapping for multiple external systems

### 🛠️ **Developer Experience**
- One-command startup with `python startup.py`
- Comprehensive testing suite
- Mock implementations for offline development
- Clear documentation and code organization

### 📈 **Scalability Ready**
- Event-driven architecture with Kafka
- Stateless workers for horizontal scaling
- Database indexing for performance
- Separation of concerns across services

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License.