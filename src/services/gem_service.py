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
import aiohttp

class GemService:
    def __init__(self, db_repository: DatabaseRepository):
        self.db_repository = db_repository
        self.logger = logging.getLogger('gem_service')
        
    async def fetch_gems(self, proxies: List[str]):
        # Create a single shared queue for gem tasks
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
        
        # Add stop signals (None) for each proxy worker
        for _ in proxies:
            await gem_task_queue.put(None)
        
        # Create workers
        workers = [
            self._worker(gem_task_queue, proxy)
            for proxy in proxies
        ]
        
        # Wait for all workers to finish
        await asyncio.gather(*workers)

    async def _worker(self, gem_task_queue: asyncio.Queue, proxy: str):
        worker_logger = WorkerLogger('gem_service', proxy)
        client = SteamPublicClient(proxy=proxy)
        items_processed = 0
        current_time = datetime.now().timestamp()
        
        try:
            while True:
                task = await gem_task_queue.get()
                if task is None:
                    gem_task_queue.task_done()
                    worker_logger.info("Stop signal received. Exiting worker loop.")
                    break
                
                gem_name, item_name_id = task
                worker_logger.set_item(gem_name)

                try:
                    # Implement a retry loop with exponential backoff
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            worker_logger.info(f"Fetching histogram (attempt {attempt+1}/{max_retries})")
                            histogram = await client.get_item_orders_histogram(item_name_id)
                            break  # Successfully retrieved, break out of retry loop
                        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                            worker_logger.warning(f"Connection error: {e}, attempt {attempt+1}/{max_retries}")
                            if attempt < max_retries - 1:
                                backoff = (2 ** attempt) * 2  # 2s, 4s, 8s
                                worker_logger.info(f"Waiting {backoff}s before retry")
                                await asyncio.sleep(backoff)
                            else:
                                worker_logger.error(f"Max retries reached, requeuing gem: {gem_name}")
                                # Put the task back in the queue for other workers
                                await gem_task_queue.put((gem_name, item_name_id))
                                # Stop processing here so a new worker can handle this item
                                break
                    else:
                        # If we never break inside for loop, something unexpected happened
                        worker_logger.error("Retry loop exited unexpectedly")
                        continue

                    # If histogram is not set, it means we failed all retries
                    if 'histogram' not in locals():
                        continue

                    # Handle data-errors: TypeError, ValueError, AttributeError
                    # outside the retry loop below:
                    if isinstance(histogram, tuple):
                        histogram = histogram[0]

                    # Process histogram and obtain new parsed buy orders
                    parsed_data = process_histogram(histogram)
                    new_orders = []
                    new_order_length = 0

                    if parsed_data and parsed_data["buy_orders"]:
                        new_orders = parsed_data["buy_orders"]
                        new_order_length = parsed_data["buy_order_length"]
                        worker_logger.info(f"Successfully parsed histogram: {new_order_length} buy orders")
                    else:
                        worker_logger.info("No buy orders found in histogram")
                    
                    # Compare with existing buy orders if available
                    existing_gem = self.db_repository.get_gem(gem_name)
                    if existing_gem is not None and existing_gem.buy_orders and new_orders:
                        try:
                            old_orders = existing_gem.parsed_buy_orders
                            if old_orders:  # if there are existing buy orders
                                # Log the first 10 new and old buy orders for comparison
                                worker_logger.info("First 10 New Buy Orders:")
                                for i, (price, qty) in enumerate(new_orders[:10], 1):
                                    worker_logger.info(f"{i}. Price: {price}, Quantity: {qty}")
                                
                                worker_logger.info("First 10 Old Buy Orders:")
                                for i, (price, qty) in enumerate(old_orders[:10], 1):
                                    worker_logger.info(f"{i}. Price: {price}, Quantity: {qty}")
                                
                                N = min(5, len(new_orders), len(old_orders))
                                differences = []
                                for i in range(N):
                                    price_old, qty_old = old_orders[i]
                                    price_new, qty_new = new_orders[i]
                                    
                                    # Calculate relative differences with more precise handling
                                    price_diff = abs(price_new - price_old) / max(abs(price_old), abs(price_new)) if price_old != 0 or price_new != 0 else 0
                                    qty_diff = abs(qty_new - qty_old) / max(abs(qty_old), abs(qty_new)) if qty_old != 0 or qty_new != 0 else 0
                                    
                                    # Log detailed comparison for each order
                                    worker_logger.info(f"Order {i+1} Comparison:")
                                    worker_logger.info(f"  Old - Price: {price_old}, Quantity: {qty_old}")
                                    worker_logger.info(f"  New - Price: {price_new}, Quantity: {qty_new}")
                                    worker_logger.info(f"  Price Difference: {price_diff*100:.2f}%")
                                    worker_logger.info(f"  Quantity Difference: {qty_diff*100:.2f}%")
                                    
                                    differences.append(max(price_diff, qty_diff))

                                # Calculate average difference
                                avg_diff = sum(differences) / N if differences else 0

                                # More precise logging of average difference
                                worker_logger.info(f"Average Order Difference: {avg_diff*100:.2f}%")

                                # Adjust threshold for considering orders different
                                if avg_diff > 0.05:  # Changed from 0.5 to 0.05 (5%)
                                    worker_logger.info(f"New histogram buy orders differ from existing by {avg_diff*100:.2f}%, keeping existing buy orders.")
                                    new_orders = old_orders
                                    new_order_length = existing_gem.buy_order_length
                        except Exception as e:
                            worker_logger.warning(f"Error comparing buy orders: {e}")
                    
                    buy_orders = str(new_orders)
                    
                    gem = Gem(
                        name=gem_name,
                        buy_orders=buy_orders,
                        buy_order_length=new_order_length,
                        timestamp=current_time
                    )
                    self.db_repository.save_gem(gem)

                except (TypeError, ValueError, AttributeError) as data_error:
                    # Data parse errors -> Save empty buy orders
                    worker_logger.error(f"Invalid histogram data: {data_error}")
                    gem = Gem(
                        name=gem_name,
                        buy_orders="[]",
                        buy_order_length=0,
                        timestamp=current_time
                    )
                    self.db_repository.save_gem(gem)
                except Exception as e:
                    worker_logger.error(f"Error processing gem: {e}", exc_info=True)
                finally:
                    gem_task_queue.task_done()
                    items_processed += 1
                    if items_processed >= 3:
                        worker_logger.debug(f"Sleeping for {settings.REQUEST_DELAY}s to rate-limit requests.")
                        await asyncio.sleep(settings.REQUEST_DELAY)
                        items_processed = 0
        finally:
            await client.session.close()
            worker_logger.info("Worker finished")
