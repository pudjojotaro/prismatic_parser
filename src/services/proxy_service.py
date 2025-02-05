import asyncio
import logging
import math
from typing import List, Tuple
from proxy_api import ProxyAPI  # type: ignore
from ..config.settings import settings

class ProxyService:
    def __init__(self):
        self.proxy_api = ProxyAPI(settings.API_KEY)
        self.current_proxy_ids = []

    async def get_proxies(self) -> List[str]:
        while True:
            try:
                proxy_data = self.proxy_api.get_all_available_proxies()
                if not proxy_data:
                    logging.warning("No available proxies found.")
                    return []

                proxy_strings = []
                self.current_proxy_ids = []

                for proxy in proxy_data:
                    formatted_proxy = self._format_proxy(proxy)
                    proxy_strings.append(formatted_proxy)
                    self.current_proxy_ids.append(proxy['id'])

                return proxy_strings
            except Exception as e:
                logging.error(f"Error fetching proxies: {e}")
                await asyncio.sleep(300)

    def _format_proxy(self, proxy: dict) -> str:
        """Format proxy dictionary into URL string."""
        if proxy.get('username') and proxy.get('password'):
            return f"{proxy['protocol']}://{proxy['username']}:{proxy['password']}@{proxy['ip']}:{proxy['port']}"
        return f"{proxy['protocol']}://{proxy['ip']}:{proxy['port']}"

    def distribute_proxies(self, proxies: List[str]) -> Tuple[List[str], List[str]]:
        total_proxies = len(proxies)
        gem_proxy_count = math.ceil(total_proxies * settings.GEM_PROXY_RATIO)
        gem_proxies = proxies[:gem_proxy_count]
        item_proxies = proxies[gem_proxy_count:]
        return gem_proxies, item_proxies

    async def unlock_proxies(self, proxy_ids):
        self.proxy_api.unlock_proxies(proxy_ids)

    async def cleanup_proxies(self):
        if self.current_proxy_ids:
            await self.unlock_proxies(self.current_proxy_ids)
            logging.info(f"Unlocked proxies: {self.current_proxy_ids}")
            self.current_proxy_ids = []
        else:
            logging.info("No proxies to unlock.")
