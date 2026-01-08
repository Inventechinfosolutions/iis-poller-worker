"""
Main application entry point for the poller worker service.
Handles service initialization, startup, and shutdown.
"""

import asyncio
import signal
import sys
from typing import Optional
import structlog
import uvicorn
from threading import Thread

from src.config.settings import settings
from src.utils.logging import configure_logging, get_logger
from src.utils.monitoring import metrics_collector, health_checker
from src.clients.database_client import database_client
from src.services.poller_worker import poller_worker
from src.api.health import app as health_app

logger = get_logger(__name__)


class PollerWorkerApplication:
    """Main application class for the poller worker service."""
    
    def __init__(self):
        self._shutdown_event = asyncio.Event()
        self._tasks = []
    
    async def initialize(self):
        """Initialize all application components."""
        try:
            logger.info("Initializing poller worker application", version=settings.version)
            
            # Initialize database client (creates tables automatically)
            await database_client.initialize()
            logger.info("Database client initialized")
            
            # Initialize poller worker
            await poller_worker.initialize()
            logger.info("Poller worker initialized")
            
            logger.info("Application initialization completed")
            
        except Exception as e:
            logger.error("Failed to initialize application", error=str(e))
            raise
    
    async def start(self):
        """Start the application services."""
        try:
            logger.info("Starting poller worker application")
            
            # Start metrics server (Prometheus metrics on port 9093)
            metrics_collector.start_metrics_server()
            
            # Start health check API server (FastAPI)
            health_api_port = getattr(settings, 'health_api_port', 3009)
            health_api_thread = Thread(
                target=lambda: uvicorn.run(
                    health_app,
                    host="0.0.0.0",
                    port=health_api_port,
                    log_level="info",
                    access_log=False
                ),
                daemon=True
            )
            health_api_thread.start()
            logger.info("Health check API started", port=health_api_port, url=f"http://localhost:{health_api_port}/health")
            
            # Start poller worker
            poller_task = asyncio.create_task(poller_worker.start())
            self._tasks.append(poller_task)
            
            # Start periodic health check task
            health_check_task = asyncio.create_task(self._periodic_health_check())
            self._tasks.append(health_check_task)
            
            logger.info("Application started successfully")
            
            # Wait for shutdown signal
            await self._shutdown_event.wait()
            
        except Exception as e:
            logger.error("Failed to start application", error=str(e))
            raise
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """Robust shutdown with graceful handling."""
        try:
            logger.info("Shutting down application")
            self._shutdown_event.set()
            
            # Give tasks time to finish gracefully
            shutdown_timeout = 30  # seconds
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=shutdown_timeout
                )
            except asyncio.TimeoutError:
                logger.warning("Shutdown timeout, cancelling tasks")
                for task in self._tasks:
                    if not task.done():
                        task.cancel()
                
                # Wait a bit more for cancellation
                await asyncio.gather(*self._tasks, return_exceptions=True)
            
            self._tasks.clear()
            
            # Stop poller worker (with timeout)
            try:
                await asyncio.wait_for(poller_worker.stop(), timeout=30)
            except asyncio.TimeoutError:
                logger.error("Poller worker shutdown timeout")
            
            # Close database connection
            await database_client.close()
            
            logger.info("Application shutdown completed")
            
        except Exception as e:
            logger.error("Error during shutdown", error=str(e), exc_info=True)
    
    async def _periodic_health_check(self):
        """Periodic health check task."""
        while not self._shutdown_event.is_set():
            try:
                health_status = await health_checker.check_health()
                logger.debug("Health check", status=health_status["status"])
                
                await asyncio.sleep(60)  # Check every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in health check", error=str(e))
                await asyncio.sleep(60)


def setup_signal_handlers(app: PollerWorkerApplication):
    """Setup signal handlers for graceful shutdown."""
    def signal_handler(signum, frame):
        logger.info("Received shutdown signal", signal=signum)
        app._shutdown_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


async def main():
    """Main entry point."""
    # Configure logging
    configure_logging()
    
    logger.info("Starting IIS Poller Worker Service", version=settings.version)
    
    # Create application instance
    app = PollerWorkerApplication()
    
    # Setup signal handlers
    setup_signal_handlers(app)
    
    try:
        # Initialize application
        await app.initialize()
        
        # Start application
        await app.start()
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error("Application error", error=str(e))
        sys.exit(1)
    finally:
        await app.shutdown()
        logger.info("Application terminated")


if __name__ == "__main__":
    asyncio.run(main())

