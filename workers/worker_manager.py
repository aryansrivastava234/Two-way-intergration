"""
Worker Manager - Orchestrates all Kafka workers and webhook server.
Handles startup, shutdown, and monitoring of all background services.
"""
import asyncio
import logging
import signal
import sys
import threading
from typing import Dict, Any, List
from datetime import datetime

from workers.outbound_worker import outbound_worker
from workers.inbound_worker import inbound_worker
from workers.webhook_server import webhook_app
from config import settings

logger = logging.getLogger(__name__)


class WorkerManager:
    """Manages all background workers and services."""
    
    def __init__(self):
        self.workers = {}
        self.webhook_server = None
        self.running = False
        self.start_time = None
        
    async def start_all_workers(self):
        """Start all workers and services."""
        logger.info("🚀 Starting Worker Manager...")
        self.start_time = datetime.utcnow()
        self.running = True
        
        # Save status to a simple file for API to read
        self._save_manager_status()
        
        try:
            # Start webhook server in separate thread
            await self._start_webhook_server()
            
            # Start Kafka workers in separate threads
            await self._start_kafka_workers()
            
            logger.info("✅ All workers started successfully")
            print("\n" + "="*60)
            print("🎉 CUSTOMER INTEGRATION SYSTEM RUNNING")
            print("="*60)
            print("📡 Webhook Server: http://localhost:8001/webhooks/stripe")
            print("📤 Outbound Worker: Processing local → Stripe events")
            print("📥 Inbound Worker: Processing Stripe → local events")
            print("⚡ Two-way sync: ACTIVE")
            print("="*60)
            
            # Keep manager running
            await self._monitor_workers()
            
        except Exception as e:
            logger.error(f"❌ Error starting workers: {e}")
            await self.stop_all_workers()
            raise
    
    async def _start_webhook_server(self):
        """Start the webhook server."""
        try:
            import uvicorn
            from threading import Thread
            
            def run_webhook_server():
                uvicorn.run(
                    webhook_app,
                    host="0.0.0.0",
                    port=8001,
                    log_level="info",
                    access_log=False
                )
            
            webhook_thread = Thread(target=run_webhook_server, daemon=True)
            webhook_thread.start()
            
            # Give server time to start
            await asyncio.sleep(2)
            
            self.workers['webhook_server'] = {
                'thread': webhook_thread,
                'type': 'webhook',
                'status': 'running',
                'started_at': datetime.utcnow()
            }
            
            logger.info("✅ Webhook server started on port 8001")
            
        except Exception as e:
            logger.error(f"❌ Failed to start webhook server: {e}")
            raise
    
    async def _start_kafka_workers(self):
        """Start all Kafka workers."""
        try:
            # Start outbound worker
            outbound_thread = threading.Thread(
                target=outbound_worker.start_worker,
                daemon=True
            )
            outbound_thread.start()
            
            self.workers['outbound_worker'] = {
                'thread': outbound_thread,
                'worker': outbound_worker,
                'type': 'kafka_outbound',
                'status': 'running',
                'started_at': datetime.utcnow()
            }
            
            # Start inbound worker
            inbound_thread = threading.Thread(
                target=inbound_worker.start_worker,
                daemon=True
            )
            inbound_thread.start()
            
            self.workers['inbound_worker'] = {
                'thread': inbound_thread,
                'worker': inbound_worker,
                'type': 'kafka_inbound',
                'status': 'running',
                'started_at': datetime.utcnow()
            }
            
            # Give workers time to initialize
            await asyncio.sleep(3)
            
            logger.info("✅ Kafka workers started")
            
            # Save updated status with workers
            self._save_manager_status()
            
        except Exception as e:
            logger.error(f"❌ Failed to start Kafka workers: {e}")
            raise
    
    async def _monitor_workers(self):
        """Monitor worker health and keep manager running."""
        try:
            while self.running:
                # Check worker health every 30 seconds
                await asyncio.sleep(30)
                await self._health_check()
                
        except asyncio.CancelledError:
            logger.info("⏸️ Worker monitoring cancelled")
        except Exception as e:
            logger.error(f"❌ Error in worker monitoring: {e}")
    
    async def _health_check(self):
        """Perform health check on all workers."""
        try:
            for name, worker_info in self.workers.items():
                thread = worker_info.get('thread')
                worker_obj = worker_info.get('worker')
                
                # Check if thread is alive
                if thread and not thread.is_alive():
                    logger.error(f"❌ Worker {name} thread has died")
                    worker_info['status'] = 'dead'
                    
                # Get stats from Kafka workers
                if worker_obj and hasattr(worker_obj, 'get_worker_stats'):
                    stats = worker_obj.get_worker_stats()
                    logger.debug(f"📊 {name} stats: {stats}")
                    
        except Exception as e:
            logger.error(f"❌ Error in health check: {e}")
    
    async def stop_all_workers(self):
        """Stop all workers and services."""
        logger.info("🛑 Stopping all workers...")
        self.running = False
        
        # Stop Kafka workers
        for name, worker_info in self.workers.items():
            try:
                worker_obj = worker_info.get('worker')
                if worker_obj and hasattr(worker_obj, 'stop_worker'):
                    logger.info(f"🛑 Stopping {name}...")
                    worker_obj.stop_worker()
                    worker_info['status'] = 'stopped'
                    
            except Exception as e:
                logger.error(f"❌ Error stopping {name}: {e}")
        
        # Give workers time to shutdown gracefully
        await asyncio.sleep(5)
        
        logger.info("✅ All workers stopped")
    
    def get_status(self) -> Dict[str, Any]:
        """Get status of all workers."""
        uptime = (datetime.utcnow() - self.start_time).total_seconds() if self.start_time else 0
        
        worker_status = {}
        for name, worker_info in self.workers.items():
            worker_obj = worker_info.get('worker')
            stats = {}
            
            if worker_obj and hasattr(worker_obj, 'get_worker_stats'):
                stats = worker_obj.get_worker_stats()
            
            worker_status[name] = {
                'type': worker_info.get('type'),
                'status': worker_info.get('status'),
                'started_at': worker_info.get('started_at').isoformat() if worker_info.get('started_at') else None,
                'stats': stats
            }
        
        status = {
            'manager_status': 'running' if self.running else 'stopped',
            'uptime_seconds': uptime,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'workers': worker_status,
            'total_workers': len(self.workers)
        }
        
        # Save status for API to read
        self._save_manager_status(status)
        return status
    
    def _save_manager_status(self, status=None):
        """Save manager status to file for API to read."""
        import json
        if status is None:
            # Build worker status properly
            worker_status = {}
            for name, worker_info in self.workers.items():
                worker_obj = worker_info.get('worker')
                stats = {}
                
                if worker_obj and hasattr(worker_obj, 'get_worker_stats'):
                    stats = worker_obj.get_worker_stats()
                
                worker_status[name] = {
                    'type': worker_info.get('type'),
                    'status': worker_info.get('status'),
                    'started_at': worker_info.get('started_at').isoformat() if worker_info.get('started_at') else None,
                    'stats': stats
                }
            
            status = {
                'manager_status': 'running' if self.running else 'stopped',
                'uptime_seconds': (datetime.utcnow() - self.start_time).total_seconds() if self.start_time else 0,
                'start_time': self.start_time.isoformat() if self.start_time else None,
                'workers': worker_status,
                'total_workers': len(self.workers)
            }
        
        try:
            with open('.worker_status.json', 'w') as f:
                json.dump(status, f)
        except Exception as e:
            logger.warning(f"Failed to save worker status: {e}")


# Global manager instance
worker_manager = WorkerManager()


async def start_all_services():
    """Start all services (main entry point)."""
    # Set up signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        logger.info(f"📡 Received signal {signum}, initiating shutdown...")
        asyncio.create_task(worker_manager.stop_all_workers())
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await worker_manager.start_all_workers()
    except KeyboardInterrupt:
        logger.info("⏸️ Received keyboard interrupt")
        await worker_manager.stop_all_workers()
    except Exception as e:
        logger.error(f"❌ Fatal error in worker manager: {e}")
        await worker_manager.stop_all_workers()
        raise


def get_worker_status():
    """Get current worker status (for API endpoints)."""
    # Try to read from shared status file first
    try:
        import json
        with open('.worker_status.json', 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        # Fallback to local instance (likely stopped)
        return worker_manager.get_status()


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("🚀 Starting Customer Integration Worker Manager...")
    
    try:
        asyncio.run(start_all_services())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)