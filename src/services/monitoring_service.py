import asyncio
import logging
from typing import Optional
from ..config.settings import settings
from ..database.repository import DatabaseRepository
from ..models.item import Item
from ..models.gem import Gem
from ..models.comparison import Comparison
from ..services.alert_service import AlertService
from datetime import datetime
from ..utils.steam_client import SteamMarketClient

class MonitoringService:
    def __init__(self, db_repository: DatabaseRepository, alert_service: AlertService):
        self.db_repository = db_repository
        self.alert_service = alert_service
        self.logger = logging.getLogger('monitoring_service')
        
    async def monitor_cycle(self):
        self.logger.info("Starting monitoring cycle")
        fetch_start, fetch_end = self.db_repository.get_last_fetch_timestamps()
        if not fetch_start or not fetch_end:
            self.logger.warning("No fetch timestamps found. Skipping monitoring cycle.")
            return
        
        items = self.db_repository.get_items_in_timerange(fetch_start, fetch_end)
        self.logger.info(f"Retrieved {len(items)} items for comparison between {fetch_start} and {fetch_end}")
        profitable_found = False
        
        for item in items:
            comparison = await self._compare_item(item)
            if comparison.is_profitable:
                profitable_found = True
                await self.alert_service.send_profit_alert(item, comparison)
                
        if not profitable_found:
            await self.alert_service.send_no_profit_alert(fetch_start, fetch_end)
        self.logger.info("Monitoring cycle completed")
        
    async def _compare_item(self, item: Item) -> Comparison:
        self.logger.info(f"\n=== Starting comparison for item ===")
        self.logger.info(f"Item ID: {item.id}")
        self.logger.info(f"Item Name: {item.name}")
        self.logger.info(f"Item Price: {item.price}")
        try:
            timestamp = float(item.timestamp)
            self.logger.info(f"Item Timestamp: {datetime.fromtimestamp(timestamp)}")
        except (ValueError, TypeError):
            self.logger.error(f"Invalid timestamp format: {item.timestamp}")
            timestamp = datetime.now().timestamp()
        
        # Log Ethereal Gem details
        if item.ethereal_gem:
            self.logger.info(f"\nEthereal Gem: {item.ethereal_gem}")
            gem = self.db_repository.get_gem(item.ethereal_gem)
            if gem:
                self.logger.info(f"Ethereal Gem Last Updated: {datetime.fromtimestamp(gem.timestamp)}")
                if gem.parsed_buy_orders:
                    self.logger.info(f"Ethereal Gem Top 3 Buy Orders:")
                    for i, (price, quantity) in enumerate(gem.parsed_buy_orders[:3]):
                        self.logger.info(f"  {i+1}. Price: {price:.2f}, Quantity: {quantity}")
            else:
                self.logger.warning(f"Ethereal Gem not found in database")
        
        # Log Prismatic Gem details
        if item.prismatic_gem:
            self.logger.info(f"\nPrismatic Gem: {item.prismatic_gem}")
            gem = self.db_repository.get_gem(item.prismatic_gem)
            if gem:
                self.logger.info(f"Prismatic Gem Last Updated: {datetime.fromtimestamp(gem.timestamp)}")
                if gem.parsed_buy_orders:
                    self.logger.info(f"Prismatic Gem Top 3 Buy Orders:")
                    for i, (price, quantity) in enumerate(gem.parsed_buy_orders[:3]):
                        self.logger.info(f"  {i+1}. Price: {price:.2f}, Quantity: {quantity}")
            else:
                self.logger.warning(f"Prismatic Gem not found in database")
        
        # Get gem prices and calculate profit
        prismatic_gem_price = await self._get_gem_price(item.prismatic_gem)
        ethereal_gem_price = await self._get_gem_price(item.ethereal_gem)
        
        combined_gem_price = 0.0
        if prismatic_gem_price is not None:
            combined_gem_price += prismatic_gem_price
        if ethereal_gem_price is not None:
            combined_gem_price += ethereal_gem_price
            
        expected_profit = combined_gem_price * (1 - settings.STEAM_FEE) - item.price
        is_profitable = expected_profit >= settings.TARGET_PROFIT
        
        comparison = Comparison(
            item_id=item.id,
            item_price=item.price,
            is_profitable=is_profitable,
            timestamp=timestamp,
            prismatic_gem_price=prismatic_gem_price,
            ethereal_gem_price=ethereal_gem_price,
            combined_gem_price=combined_gem_price,
            expected_profit=expected_profit
        )
        self.db_repository.save_comparison(comparison)
        self.logger.debug(
            f"Compared item {item.id}: Expected Profit = {expected_profit:.2f}, Is Profitable = {is_profitable}"
        )
        
        if comparison.is_profitable:
            await self.buy_profitable_item(comparison)
        
        return comparison

    async def _get_gem_price(self, gem_name: Optional[str]) -> Optional[float]:
        if not gem_name:
            return None
        
        gem = self.db_repository.get_gem(gem_name)
        if not gem:
            self.logger.warning(f"Gem '{gem_name}' not found in database")
            return None
        
        buy_orders = gem.parsed_buy_orders
        if not buy_orders:
            self.logger.warning(f"No buy orders found for gem '{gem_name}'")
            return None
        
        return buy_orders[0][0]

    async def buy_profitable_item(self, comparison: Comparison) -> bool:
        self.logger.info(f"Attempting to buy profitable item {comparison.item_id}")
        
        # Get the raw listing from database
        raw_listing = self.db_repository.get_raw_listing(comparison.item_id)
        if not raw_listing:
            self.logger.error(f"Raw listing not found for item {comparison.item_id}")
            return False
        
        try:
            # Initialize Steam client
            steam_client = SteamMarketClient()
            await steam_client.initialize()
            
            # Attempt to buy the listing
            wallet_info = await steam_client.buy_listing(raw_listing)
            
            # Log success and notify
            self.logger.info(f"Successfully bought item {comparison.item_id}. Wallet info: {wallet_info}")
            await self.alert_service.send_message(
                f"🎉 Successfully bought profitable item!\n"
                f"Item: {raw_listing.item.description.market_name}\n"
                f"Price: {comparison.item_price}\n"
                f"Expected Profit: {comparison.expected_profit}"
            )
            return True
        
        except Exception as e:
            self.logger.error(f"Failed to buy item {comparison.item_id}: {e}")
            await self.alert_service.send_message(
                f"❌ Failed to buy profitable item!\n"
                f"Item: {comparison.item_id}\n"
                f"Error: {str(e)}"
            )
            return False
        
        finally:
            if steam_client:
                await steam_client.close()
