import asyncio
import logging
import json
from pathlib import Path
from typing import Optional, Tuple, List, Any
from aiosteampy import SteamClient, Currency, App, AppContext
from aiosteampy.helpers import restore_from_cookies
from aiosteampy.utils import get_jsonable_cookies
from aiosteampy.models import MarketListing
import aiohttp
from ..config.settings import settings

class SteamMarketClient:
    def __init__(
        self, 
        config_path: str = str(settings.STEAM_CONFIG_PATH),
        cookies_path: str = str(settings.STEAM_COOKIES_PATH),
        logging_level: int = logging.INFO
    ):
        self.config_path = Path(config_path)
        self.cookies_path = Path(cookies_path)
        self.client: Optional[SteamClient] = None
        
        # Configure logging
        logging.basicConfig(level=logging_level)
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def get_random_user_agent() -> str:
        return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"

    async def save_session_cookies(self) -> None:
        if not self.client:
            raise ValueError("Client not initialized")
        
        cookies = get_jsonable_cookies(self.client.session)
        with open(self.cookies_path, "w") as f:
            json.dump(cookies, f)
        self.logger.info("Session cookies saved.")

    async def load_session_and_login(self) -> None:
        if not self.client:
            raise ValueError("Client not initialized")

        try:
            with open(self.cookies_path, "r") as f:
                cookies = json.load(f)
            await restore_from_cookies(cookies, self.client)
            if not await self.client.is_session_alive():
                self.logger.info("Session not alive. Logging in...")
                await self.client.login()
            else:
                self.logger.info("Session restored successfully.")
        except FileNotFoundError:
            self.logger.info("No cookies file found. Logging in...")
            await self.client.login()
            await self.save_session_cookies()

    def load_config(self) -> dict:
        try:
            with open(self.config_path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.error(f"Configuration file '{self.config_path}' not found.")
            raise
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON in configuration file.")
            raise

    async def initialize(self) -> None:
        config = self.load_config()
        
        self.client = SteamClient(
            steam_id=config["steam_id"],
            username=config["username"],
            password=config["password"],
            shared_secret=config["shared_secret"],
            identity_secret=config["identity_secret"],
            wallet_currency=Currency[config["wallet_currency"]],
            user_agent=self.get_random_user_agent(),
        )
        
        await self.load_session_and_login()

    async def buy_listing(self, listing: Any) -> dict:
        """
        Attempt to buy a market listing.
        
        Args:
            listing: The market listing object to purchase. Can be a MarketListing object
                    or any object that the Steam client can process for purchase.
            
        Returns:
            dict: Wallet info after purchase
            
        Raises:
            ValueError: If client is not initialized
            aiohttp.ClientResponseError: If the purchase request fails
            Exception: For any other unexpected errors
        """
        if not self.client:
            raise ValueError("Client not initialized")

        try:
            self.logger.info(f"Attempting to buy listing: {listing}")
            wallet_info = await self.client.buy_market_listing(listing)
            self.logger.info(f"Successfully bought the listing: {wallet_info}")
            return wallet_info
        except aiohttp.ClientResponseError as e:
            self.logger.error(
                f"Failed to buy listing. Status: {e.status}, Message: {e.message}, URL: {e.request_info.url}"
            )
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error while buying listing: {str(e)}")
            raise

    async def close(self) -> None:
        """Close the client session."""
        if self.client:
            await self.save_session_cookies()
            await self.client.session.close()
            self.logger.info("Session closed.") 