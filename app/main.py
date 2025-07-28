"""
Main FastAPI application with webhook + Kafka worker integration.
Phase 5B + 6: Complete two-way sync with webhooks and real Kafka workers.
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.database import create_tables
from app.schemas import APIResponse
from app.api import customers
from app.kafka_client import event_publisher
from workers.worker_manager import get_worker_status
from config import settings

# Worker manager will be started separately
# This API focuses on customer operations


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager - handles startup and shutdown."""
    
    # Startup
    print("🚀 Starting Customer Integration API...")
    
    # Create database tables
    create_tables()
    print("✅ Database tables created/verified")
    
    print("🎉 API startup complete!")
    print("ℹ️  Note: Start workers separately with: python -m workers.worker_manager")
    
    yield
    
    # Shutdown
    print("🛑 Shutting down Customer Integration API...")
    
    # Close Kafka producer
    event_publisher.close()
    print("✅ Kafka producer closed")
    
    print("👋 API shutdown complete!")


# Create FastAPI app with lifespan
app = FastAPI(
    title="Customer Integration API with Two-Way Sync",
    description="Complete two-way customer synchronization with Stripe via webhooks and Kafka workers",
    version="2.0.0",
    debug=settings.debug,
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include customer API routes
app.include_router(customers.router, prefix="/customers", tags=["customers"])


@app.get("/", response_model=APIResponse)
async def root():
    """Root endpoint."""
    return APIResponse(
        success=True,
        message="Customer Integration API with Two-Way Sync is running",
        data={
            "version": "2.0.0",
            "architecture": "webhook + kafka_workers",
            "features": [
                "real_time_webhooks",
                "kafka_workers", 
                "conflict_resolution",
                "two_way_sync"
            ]
        }
    )


@app.get("/health")
async def health_check():
    """Health check endpoint with comprehensive status."""
    from app.integrations import integration_manager
    
    stripe_integration = integration_manager.get_integration('stripe')
    stripe_status = stripe_integration.test_connection() if stripe_integration else False
    
    # Get worker status
    worker_status = get_worker_status()
    
    return {
        "status": "healthy",
        "timestamp": worker_status.get('start_time'),
        "uptime_seconds": worker_status.get('uptime_seconds', 0),
        "api": {
            "version": settings.app_version,
            "debug": settings.debug
        },
        "integrations": {
            "stripe": {
                "available": stripe_integration is not None,
                "connected": stripe_status,
                "type": stripe_integration.__class__.__name__ if stripe_integration else None
            }
        },
        "workers": worker_status.get('workers', {}),
        "database": {
            "configured": bool(settings.database.url),
            "host": settings.database.host
        },
        "kafka": {
            "configured": bool(settings.kafka.bootstrap_servers),
            "servers": settings.kafka.bootstrap_servers
        },
        "stripe_config": {
            "secret_key_configured": bool(settings.stripe.secret_key),
            "webhook_secret_configured": bool(settings.stripe.webhook_secret),
            "webhook_url": settings.stripe.webhook_url
        }
    }



# 🆕 WORKER MANAGEMENT ENDPOINTS
@app.get("/workers/status")
async def get_workers_status():
    """Get status of all background workers."""
    return get_worker_status()


@app.get("/workers/health")
async def workers_health_check():
    """Health check specifically for workers."""
    worker_status = get_worker_status()
    
    all_healthy = (
        worker_status.get('manager_status') == 'running' and
        len([w for w in worker_status.get('workers', {}).values() 
             if w.get('status') == 'running']) >= 2
    )
    
    return {
        "healthy": all_healthy,
        "manager_running": worker_status.get('manager_status') == 'running',
        "active_workers": len([w for w in worker_status.get('workers', {}).values() 
                              if w.get('status') == 'running']),
        "total_workers": worker_status.get('total_workers', 0),
        "uptime_seconds": worker_status.get('uptime_seconds', 0)
    }


# 🆕 SYNC ANALYTICS ENDPOINTS
@app.get("/sync/analytics")
async def get_sync_analytics():
    """Get synchronization analytics and statistics."""
    worker_status = get_worker_status()
    
    analytics = {
        "system_status": {
            "two_way_sync_active": worker_status.get('manager_status') == 'running',
            "uptime_seconds": worker_status.get('uptime_seconds', 0)
        },
        "worker_stats": {},
        "sync_methods": {
            "outbound": "kafka_workers",
            "inbound": "webhooks + kafka_workers",
            "conflict_resolution": "first_write_wins"
        }
    }
    
    # Extract worker statistics
    for worker_name, worker_info in worker_status.get('workers', {}).items():
        stats = worker_info.get('stats', {})
        if stats:
            analytics['worker_stats'][worker_name] = {
                "processed_count": stats.get('processed_count', 0),
                "error_count": stats.get('error_count', 0),
                "running": stats.get('running', False)
            }
    
    return analytics


@app.get("/sync/test")
async def test_sync_system():
    """Test the synchronization system end-to-end."""
    from app.integrations import integration_manager
    
    # Test components
    stripe_integration = integration_manager.get_integration('stripe')
    stripe_connected = stripe_integration.test_connection() if stripe_integration else False
    
    worker_status = get_worker_status()
    workers_running = worker_status.get('manager_status') == 'running'
    
    kafka_producer_connected = event_publisher.connected
    
    return {
        "system_ready": stripe_connected and workers_running and kafka_producer_connected,
        "components": {
            "stripe_integration": {
                "connected": stripe_connected,
                "type": stripe_integration.__class__.__name__ if stripe_integration else None
            },
            "kafka_producer": {
                "connected": kafka_producer_connected
            },
            "workers": {
                "running": workers_running,
                "count": len([w for w in worker_status.get('workers', {}).values() 
                            if w.get('status') == 'running'])
            }
        },
        "sync_flow": {
            "outbound": "API → Kafka → Worker → Stripe",
            "inbound": "Stripe → Webhook → Kafka → Worker → Database"
        }
    }


if __name__ == "__main__":
    import uvicorn
    print("🚀 Starting Customer Integration API...")
    print("ℹ️  Remember to start workers: python -m workers.worker_manager")
    
    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug
    )