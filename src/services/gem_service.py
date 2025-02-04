import asyncio
import logging
from datetime import datetime
from typing import List
from aiosteampy import SteamPublicClient
from ..config.settings import settings
from ..config.constants import ALLOWED_GEMS_ETHEREAL, ALLOWED_GEMS_PRISMATIC
from ..database.repository import DatabaseRepository
from ..models.gem import Gem
from ..utils.parsing import process_histogram
from ..utils.worker_logger import WorkerLogger
import json

class GemService:
    def __init__(self, db_repository: DatabaseRepository):
        self.db_repository = db_repository
        self.logger = logging.getLogger('gem_service')
        
    async def fetch_gems(self, proxies: List[str]):
        gem_task_queue = asyncio.Queue()
        
        # Load both gem types
        with open("src/python_helpers/gems_prismatic_with_ID.json", "r") as f:
            prismatic_ids = json.load(f)
        with open("src/python_helpers/gems_ethereal_with_ID.json", "r") as f:
            ethereal_ids = json.load(f)
        
        # Queue ethereal gems
        for gem_name in ALLOWED_GEMS_ETHEREAL:
            full_name = f"Ethereal: {gem_name}"
            if full_name in ethereal_ids:
                await gem_task_queue.put((gem_name, ethereal_ids[full_name]["id"]))
        
        # Queue prismatic gems
        for gem_name in ALLOWED_GEMS_PRISMATIC:
            full_name = f"Prismatic: {gem_name}"
            if full_name in prismatic_ids:
                await gem_task_queue.put((gem_name, prismatic_ids[full_name]["id"]))
        
        # Create workers with delay between each
        workers = []
        for i, proxy in enumerate(proxies):
            worker = self._worker(gem_task_queue, proxy)
            workers.append(worker)
            if i < len(proxies) - 1:  # Don't delay after the last worker
                await asyncio.sleep(2)  # Stagger worker starts
        
        await asyncio.gather(*workers)
        
    async def _worker(self, gem_task_queue: asyncio.Queue, proxy: str):
        worker_logger = WorkerLogger('gem_service', proxy)
        max_retries = 3
        retry_delay = 5
        items_processed = 0
        current_time = datetime.now().timestamp()
        
        while not gem_task_queue.empty():
            client = None
            try:
                client = SteamPublicClient(proxy=proxy)
                gem_name, item_name_id = await gem_task_queue.get()
                worker_logger.set_item(gem_name)
                
                # Retry logic for histogram fetching
                for attempt in range(max_retries):
                    try:
                        worker_logger.info("Fetching histogram")
                        histogram = await client.get_item_orders_histogram(item_name_id)
                        
                        if isinstance(histogram, tuple):
                            histogram = histogram[0]
                        
                        parsed_data = process_histogram(histogram)
                        buy_orders = "[]"
                        buy_order_length = 0
                        
                        if parsed_data is not None and parsed_data["buy_orders"]:
                            buy_orders = str(parsed_data["buy_orders"])
                            buy_order_length = parsed_data["buy_order_length"]
                            worker_logger.info(f"Successfully parsed histogram: {buy_order_length} buy orders")
                        else:
                            worker_logger.info("No buy orders found in histogram")
                        
                        gem = Gem(
                            name=gem_name,
                            buy_orders=buy_orders,
                            buy_order_length=buy_order_length,
                            timestamp=current_time
                        )
                        self.db_repository.save_gem(gem)
                        break  # Success, exit retry loop
                        
                    except (TypeError, ValueError, AttributeError) as e:
                        worker_logger.error(f"Invalid histogram data: {str(e)}")
                        break  # Data parsing error, no retry needed
                    except Exception as e:
                        if attempt < max_retries - 1:
                            worker_logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                            await asyncio.sleep(retry_delay * (attempt + 1))
                            # Create new client for retry
                            if client:
                                await client.session.close()
                            client = SteamPublicClient(proxy=proxy)
                        else:
                            worker_logger.error(f"Failed after {max_retries} attempts: {str(e)}")
                
                # Rate limiting
                items_processed += 1
                if items_processed >= 2:  # Reduced from 3 to 2 for more conservative rate limiting
                    delay = settings.REQUEST_DELAY * 1.5  # Increased delay
                    worker_logger.debug(f"Sleeping for {delay}s")
                    await asyncio.sleep(delay)
                    items_processed = 0
                else:
                    # Small delay between requests even within the batch
                    await asyncio.sleep(1)
                
            except Exception as e:
                worker_logger.error(f"Unexpected error in worker: {str(e)}", exc_info=True)
            finally:
                if client:
                    try:
                        await client.session.close()
                    except Exception as e:
                        worker_logger.error(f"Error closing client session: {str(e)}")
                if not gem_task_queue.empty():
                    gem_task_queue.task_done()
        
        worker_logger.info("Worker finished")
