#!/usr/bin/env python3
"""
Customer Integration System Startup Manager

This module provides a comprehensive startup script for the Customer Integration System,
orchestrating the initialization and monitoring of all required services including:
- PostgreSQL and Kafka infrastructure via Docker Compose
- FastAPI server for REST endpoints
- Kafka workers for event processing
- Stripe webhook handlers

Author: Zenskar Assignment
Version: 1.0.0
"""

import subprocess
import sys
import time
import signal
import logging
from pathlib import Path
from typing import Dict, Any, Optional


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('startup.log')
    ]
)

logger = logging.getLogger(__name__)


class TerminalColors:
    """ANSI color codes for terminal output formatting."""
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    END = '\033[0m'


def print_colored(text: str, color: str) -> None:
    """Print colored text to terminal."""
    print(f"{color}{text}{TerminalColors.END}")


def display_startup_banner() -> None:
    """Display application startup banner with system information."""
    banner_width = 80
    print("\n" + "=" * banner_width)
    print_colored("🚀 CUSTOMER INTEGRATION SYSTEM", TerminalColors.BOLD + TerminalColors.BLUE)
    print("=" * banner_width)
    print_colored("System Features:", TerminalColors.BOLD)
    print("  • Bidirectional customer synchronization with Stripe")
    print("  • Real-time webhook processing for instant updates")
    print("  • Apache Kafka workers for reliable message processing")
    print("  • Conflict resolution using first-write-wins strategy")
    print("  • RESTful API with comprehensive customer management")
    print("  • Health monitoring and graceful shutdown capabilities")
    print("=" * banner_width)

class SystemManager:
    """
    Manages the lifecycle of all system services including infrastructure,
    API server, and background workers.
    """
    
    def __init__(self) -> None:
        """Initialize the system manager."""
        self.processes: Dict[str, Dict[str, Any]] = {}
        self.running: bool = True
        logger.info("SystemManager initialized")
        
    def start_process(self, name: str, command: str, cwd: Optional[str] = None) -> bool:
        """
        Start a subprocess with monitoring capabilities.
        
        Args:
            name: Unique identifier for the process
            command: Shell command to execute
            cwd: Working directory for the process
            
        Returns:
            bool: True if process started successfully, False otherwise
        """
        try:
            logger.info(f"Starting process: {name}")
            print_colored(f"🚀 Starting {name}...", TerminalColors.BLUE)
            
            process = subprocess.Popen(
                command,
                shell=True,
                cwd=cwd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )
            
            self.processes[name] = {
                'process': process,
                'command': command,
                'started_at': time.time(),
                'cwd': cwd
            }
            
            logger.info(f"Process {name} started with PID: {process.pid}")
            print_colored(f"✅ {name} started (PID: {process.pid})", TerminalColors.GREEN)
            return True
            
        except Exception as e:
            logger.error(f"Failed to start process {name}: {str(e)}")
            print_colored(f"❌ Failed to start {name}: {e}", TerminalColors.RED)
            return False
    
    def check_dependencies(self) -> bool:
        """
        Verify that all required dependencies are available.
        
        Returns:
            bool: True if all dependencies are satisfied, False otherwise
        """
        logger.info("Checking system dependencies")
        print_colored("🔍 Checking dependencies...", TerminalColors.BLUE)
        
        # Check Docker availability and status
        try:
            result = subprocess.run(
                ['docker', 'ps'], 
                capture_output=True, 
                text=True, 
                timeout=10
            )
            if result.returncode != 0:
                error_msg = "Docker daemon not running - please start Docker first"
                logger.warning(error_msg)
                print_colored(f"⚠️  {error_msg}", TerminalColors.YELLOW)
                return False
        except FileNotFoundError:
            error_msg = "Docker not found - please install Docker"
            logger.error(error_msg)
            print_colored(f"⚠️  {error_msg}", TerminalColors.YELLOW)
            return False
        except subprocess.TimeoutExpired:
            error_msg = "Docker command timed out - Docker may be unresponsive"
            logger.error(error_msg)
            print_colored(f"⚠️  {error_msg}", TerminalColors.YELLOW)
            return False
        
        # Verify docker-compose configuration exists
        compose_file = Path('docker-compose.yml')
        if not compose_file.exists():
            error_msg = "docker-compose.yml not found in current directory"
            logger.error(error_msg)
            print_colored(f"❌ {error_msg}", TerminalColors.RED)
            return False
        
        # Check docker-compose availability
        try:
            subprocess.run(
                ['docker-compose', '--version'], 
                capture_output=True, 
                check=True,
                timeout=5
            )
        except (FileNotFoundError, subprocess.CalledProcessError):
            error_msg = "docker-compose not available - please install Docker Compose"
            logger.error(error_msg)
            print_colored(f"⚠️  {error_msg}", TerminalColors.YELLOW)
            return False
        except subprocess.TimeoutExpired:
            error_msg = "docker-compose command timed out"
            logger.error(error_msg)
            print_colored(f"⚠️  {error_msg}", TerminalColors.YELLOW)
            return False
        
        logger.info("All dependencies satisfied")
        print_colored("✅ Dependencies check passed", TerminalColors.GREEN)
        return True
    
    def start_infrastructure(self) -> bool:
        """
        Initialize and start Docker infrastructure services.
        
        Returns:
            bool: True if infrastructure started successfully, False otherwise
        """
        logger.info("Starting Docker infrastructure")
        print_colored("🐳 Starting Docker infrastructure...", TerminalColors.BLUE)
        
        try:
            # Create containers without starting them
            logger.info("Creating Docker containers")
            print_colored("📦 Creating containers...", TerminalColors.YELLOW)
            create_result = subprocess.run(
                ["docker-compose", "up", "--no-start"],
                cwd=".",
                capture_output=True,
                text=True,
                timeout=120
            )
            
            if create_result.returncode != 0:
                error_msg = f"Failed to create containers: {create_result.stderr}"
                logger.error(error_msg)
                print_colored(f"❌ {error_msg}", TerminalColors.RED)
                return False
            
            # Start the containers
            logger.info("Starting Docker containers")
            print_colored("🚀 Starting containers...", TerminalColors.YELLOW)
            start_result = subprocess.run(
                ["docker-compose", "start"],
                cwd=".",
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if start_result.returncode != 0:
                error_msg = f"Failed to start containers: {start_result.stderr}"
                logger.error(error_msg)
                print_colored(f"❌ {error_msg}", TerminalColors.RED)
                return False
            
            # Wait for services to initialize
            print_colored("⏳ Waiting for services to initialize...", TerminalColors.YELLOW)
            logger.info("Waiting for services to be ready")
            time.sleep(15)
            
            logger.info("Infrastructure startup completed")
            print_colored("✅ Infrastructure ready", TerminalColors.GREEN)
            return True
            
        except subprocess.TimeoutExpired:
            error_msg = "Docker operations timed out"
            logger.error(error_msg)
            print_colored(f"❌ {error_msg}", TerminalColors.RED)
            return False
        except Exception as e:
            error_msg = f"Unexpected error during infrastructure startup: {str(e)}"
            logger.error(error_msg)
            print_colored(f"❌ {error_msg}", TerminalColors.RED)
            return False
    
    def run_migrations(self) -> None:
        """
        Execute database migration scripts if they exist.
        
        Note: This is a placeholder implementation. In production,
        migrations would be executed against the actual database.
        """
        logger.info("Running database migrations")
        print_colored("🗄️  Running database migrations...", TerminalColors.BLUE)
        
        migration_files = [
            "migrations/001_add_sync_state_table.sql",
            "migrations/002_add_processed_events_table.sql"
        ]
        
        migrations_dir = Path("migrations")
        if not migrations_dir.exists():
            logger.info("No migrations directory found, skipping migrations")
            print_colored("ℹ️  No migrations directory found", TerminalColors.YELLOW)
            return
        
        executed_count = 0
        for migration_file in migration_files:
            migration_path = Path(migration_file)
            if migration_path.exists():
                logger.info(f"Would execute migration: {migration_file}")
                print_colored(f"📝 Migration found: {migration_file}", TerminalColors.YELLOW)
                executed_count += 1
        
        if executed_count == 0:
            print_colored("ℹ️  No migration files found", TerminalColors.YELLOW)
        else:
            print_colored(f"✅ Migration check completed ({executed_count} files)", TerminalColors.GREEN)
        
        logger.info(f"Migration check completed, {executed_count} files found")
    
    def start_all_services(self) -> bool:
        """
        Start all application services in the correct order.
        
        Returns:
            bool: True if all services started successfully, False otherwise
        """
        logger.info("Starting application services")
        
        # Start API server
        logger.info("Starting FastAPI server")
        api_success = self.start_process(
            "api_server",
            "python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload",
            cwd="."
        )
        
        if not api_success:
            logger.error("Failed to start API server")
            return False
        
        # Allow API server to initialize
        logger.info("Waiting for API server to initialize")
        time.sleep(3)
        
        # Start worker manager for Kafka and webhook processing
        logger.info("Starting worker manager")
        worker_success = self.start_process(
            "worker_manager",
            "python -m workers.worker_manager",
            cwd="."
        )
        
        if not worker_success:
            logger.error("Failed to start worker manager")
            return False
        
        success = api_success and worker_success
        if success:
            logger.info("All services started successfully")
        else:
            logger.error("Some services failed to start")
            
        return success
    
    def monitor_processes(self) -> None:
        """
        Monitor all running processes and handle graceful shutdown.
        
        This method runs in a loop, checking process health and responding
        to shutdown signals (Ctrl+C).
        """
        logger.info("Starting process monitoring")
        print_colored("\n📊 System is running! Monitoring processes...", TerminalColors.GREEN)
        print_colored("Press Ctrl+C to shutdown all services", TerminalColors.YELLOW)
        
        try:
            while self.running:
                time.sleep(5)
                
                # Check health of all managed processes
                for name, info in self.processes.items():
                    process = info['process']
                    if process.poll() is not None:
                        logger.warning(f"Process {name} has stopped unexpectedly")
                        print_colored(f"❌ Process {name} has stopped unexpectedly", TerminalColors.RED)
                        # TODO: Implement automatic restart logic if needed
                        
        except KeyboardInterrupt:
            logger.info("Shutdown signal received")
            print_colored("\n⏸️  Shutdown signal received", TerminalColors.YELLOW)
            self.shutdown_all()
    
    def shutdown_all(self) -> None:
        """
        Gracefully shutdown all managed processes and infrastructure.
        
        This method ensures all processes are terminated cleanly and
        Docker infrastructure is properly stopped.
        """
        logger.info("Initiating system shutdown")
        print_colored("🛑 Shutting down all services...", TerminalColors.YELLOW)
        self.running = False
        
        # Terminate all managed processes
        for name, info in self.processes.items():
            try:
                process = info['process']
                if process.poll() is None:  # Process is still running
                    logger.info(f"Stopping process: {name}")
                    print_colored(f"🛑 Stopping {name}...", TerminalColors.YELLOW)
                    process.terminate()
                    
                    # Allow graceful shutdown
                    try:
                        process.wait(timeout=10)
                        logger.info(f"Process {name} stopped gracefully")
                        print_colored(f"✅ {name} stopped gracefully", TerminalColors.GREEN)
                    except subprocess.TimeoutExpired:
                        logger.warning(f"Force killing process: {name}")
                        print_colored(f"⚠️  Force killing {name}...", TerminalColors.YELLOW)
                        process.kill()
                        process.wait()  # Ensure process is fully terminated
                        
            except Exception as e:
                logger.error(f"Error stopping process {name}: {str(e)}")
                print_colored(f"❌ Error stopping {name}: {e}", TerminalColors.RED)
        
        # Shutdown Docker infrastructure
        try:
            logger.info("Stopping Docker infrastructure")
            print_colored("🐳 Stopping Docker infrastructure...", TerminalColors.BLUE)
            subprocess.run(
                ['docker-compose', 'down'], 
                capture_output=True, 
                timeout=30
            )
            logger.info("Docker infrastructure stopped")
        except subprocess.TimeoutExpired:
            logger.warning("Docker shutdown timed out")
        except Exception as e:
            logger.error(f"Error stopping Docker infrastructure: {str(e)}")
        
        logger.info("System shutdown completed")
        print_colored("👋 System shutdown complete!", TerminalColors.GREEN)
    
    def display_access_information(self) -> None:
        """
        Display comprehensive access information and next steps.
        
        Shows all available endpoints, configuration instructions,
        and testing guidance for the running system.
        """
        try:
            # Import settings to get actual webhook URL
            from config.settings import settings
            webhook_url = settings.stripe.webhook_url or "http://localhost:8001"
            webhook_endpoint = f"{webhook_url}/webhooks/stripe"
        except ImportError:
            logger.warning("Could not import settings, using default webhook URL")
            webhook_endpoint = "http://localhost:8001/webhooks/stripe"
        
        info_width = 60
        print("\n" + "=" * info_width)
        print_colored("🌐 SYSTEM ACCESS INFORMATION", TerminalColors.BOLD + TerminalColors.GREEN)
        print("=" * info_width)
        
        # Core endpoints
        print_colored("Core Services:", TerminalColors.BOLD)
        print("📡 Customer API:      http://localhost:8000")
        print("📖 API Documentation: http://localhost:8000/docs")
        print("💾 Health Check:     http://localhost:8000/health")
        
        # Monitoring endpoints
        print_colored("\nMonitoring:", TerminalColors.BOLD)
        print("📊 Worker Status:    http://localhost:8000/workers/status")
        print("🧪 Sync Test:        http://localhost:8000/sync/test")
        
        # Integration endpoints
        print_colored("\nIntegration:", TerminalColors.BOLD)
        print(f"🔗 Webhook Endpoint: {webhook_endpoint}")
        
        print("=" * info_width)
        print_colored("💡 NEXT STEPS:", TerminalColors.BOLD + TerminalColors.YELLOW)
        print(f"1. Configure Stripe webhook URL: {webhook_endpoint}")
        print("2. Test customer creation: POST http://localhost:8000/customers")
        print("3. Monitor system health: http://localhost:8000/workers/status")
        print("4. View API documentation: http://localhost:8000/docs")
        print("=" * info_width + "\n")
        
        logger.info("Access information displayed")


def main() -> int:
    """
    Main application entry point.
    
    Orchestrates the complete system startup sequence including:
    1. Dependency verification
    2. Infrastructure initialization
    3. Database migrations
    4. Service startup
    5. Process monitoring
    
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    display_startup_banner()
    logger.info("Starting Customer Integration System")
    
    manager = SystemManager()
    
    # Setup signal handlers for graceful shutdown
    def signal_handler(signum: int, frame) -> None:
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, initiating shutdown")
        print_colored("\n📡 Received shutdown signal", TerminalColors.YELLOW)
        manager.shutdown_all()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Phase 1: Dependency verification
        logger.info("Phase 1: Checking dependencies")
        if not manager.check_dependencies():
            logger.error("Dependency check failed")
            print_colored("❌ Dependency check failed", TerminalColors.RED)
            return 1
        
        # Phase 2: Infrastructure startup
        logger.info("Phase 2: Starting infrastructure")
        if not manager.start_infrastructure():
            logger.error("Infrastructure startup failed")
            print_colored("❌ Failed to start infrastructure", TerminalColors.RED)
            return 1
        
        # Phase 3: Database migrations
        logger.info("Phase 3: Running migrations")
        manager.run_migrations()
        
        # Phase 4: Application services
        logger.info("Phase 4: Starting application services")
        if not manager.start_all_services():
            logger.error("Service startup failed")
            print_colored("❌ Failed to start services", TerminalColors.RED)
            manager.shutdown_all()
            return 1
        
        # Phase 5: Display access information
        manager.display_access_information()
        
        # Phase 6: Process monitoring
        logger.info("Phase 6: Starting process monitoring")
        manager.monitor_processes()
        
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        manager.shutdown_all()
        return 0
    except Exception as e:
        logger.error(f"Fatal error during startup: {str(e)}", exc_info=True)
        print_colored(f"❌ Fatal error: {e}", TerminalColors.RED)
        manager.shutdown_all()
        return 1
    
    logger.info("System startup completed successfully")
    return 0


if __name__ == "__main__":
    exit_code = main()
    logger.info(f"Application exiting with code: {exit_code}")
    sys.exit(exit_code)