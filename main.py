import asyncio
import logging
from datetime import datetime
from src.config.settings import settings
from src.database.models import init_db
from src.database.repository import DatabaseRepository
from src.services.item_service import ItemService
from src.services.gem_service import GemService
from src.services.monitoring_service import MonitoringService
from src.services.alert_service import AlertService
from src.services.proxy_service import ProxyService
from src.utils.logging import setup_logging

class Application:
    def __init__(self, item_service: ItemService, gem_service: GemService, 
                 monitoring_service: MonitoringService, alert_service: AlertService,
                 proxy_service: ProxyService):
        self.item_service = item_service
        self.gem_service = gem_service
        self.monitoring_service = monitoring_service
        self.alert_service = alert_service
        self.proxy_service = proxy_service
        self.logger = logging.getLogger('main')
        
    async def run(self):
        # Start Telegram bot polling as a background task
        bot_task = asyncio.create_task(self.alert_service.run_bot())
        await self.alert_service.send_startup_message()
        try:
            while True:
                try:
                    self.logger.info("Starting new fetch cycle")
                    cycle_start = datetime.now()

                    # Get and distribute proxies
                    proxies = await self.proxy_service.get_proxies()
                    if not proxies:
                        self.logger.warning("No available proxies found.")
                        await asyncio.sleep(settings.ERROR_DELAY)
                        continue

                    gem_proxies, item_proxies = self.proxy_service.distribute_proxies(proxies)

                    try:
                        # Run item_service and gem_service concurrently
                        await asyncio.gather(
                            self.item_service.fetch_items(item_proxies),
                            self.gem_service.fetch_gems(gem_proxies)
                        )
                        self.logger.info("Fetch services completed")

                        # Run monitoring_service after both services are complete
                        await self.monitoring_service.monitor_cycle()
                        self.logger.info("Monitoring service completed")
                    finally:
                        # Ensure proxies are always unlocked
                        await self.proxy_service.cleanup_proxies()
                        self.logger.info("Proxies unlocked")

                    self.logger.info(f"Waiting {settings.CYCLE_INTERVAL:.2f} seconds until next cycle")
                    await asyncio.sleep(settings.CYCLE_INTERVAL)

                except Exception as e:
                    self.logger.error(f"Error during cycle: {str(e)}", exc_info=True)
                    await asyncio.sleep(settings.ERROR_DELAY)

        except asyncio.CancelledError:
            await self.alert_service.send_shutdown_message()
            self.logger.info("Received cancellation request. Shutting down...")
            # Cancel bot task
            if not bot_task.done():
                bot_task.cancel()
                try:
                    await bot_task
                except asyncio.CancelledError:
                    pass
            # No break needed here, just let the function complete naturally
        finally:
            # Ensure final cleanup
            try:
                await self.proxy_service.cleanup_proxies()
            except Exception as e:
                self.logger.error(f"Error during final cleanup: {str(e)}", exc_info=True)

async def main():
    setup_logging()
    logger = logging.getLogger('main')
    logger.info("Starting application")
    init_db()
    
    db_repository = DatabaseRepository()
    alert_service = AlertService()
    proxy_service = ProxyService()
    item_service = ItemService(db_repository)
    gem_service = GemService(db_repository)
    monitoring_service = MonitoringService(db_repository, alert_service)
    
    app = Application(item_service, gem_service, monitoring_service, alert_service, proxy_service)

    # Create the main task
    main_task = asyncio.create_task(app.run())

    # Handle Ctrl+C
    try:
        await main_task
    except KeyboardInterrupt:
        logger.info("Received exit signal (Ctrl+C). Cancelling tasks...")
        # Cancel the main task and wait for it to complete
        main_task.cancel()
        try:
            await main_task  # This will trigger the CancelledError in app.run()
        except asyncio.CancelledError:
            pass
        logger.info("Shutdown complete.")

if __name__ == "__main__":
    asyncio.run(main()) 