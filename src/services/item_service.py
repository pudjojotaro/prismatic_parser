import asyncio
import logging
from typing import List
from aiosteampy import SteamPublicClient, App, Currency
from ..config.settings import settings
from ..config.constants import ITEMS, COURIERS, ALLOWED_GEMS_ETHEREAL, ALLOWED_GEMS_PRISMATIC
from ..database.repository import DatabaseRepository
from ..models.item import Item
from ..utils.parsing import parse_market_listings
from ..utils.worker_logger import WorkerLogger
from datetime import datetime
import pandas as pd

class ItemService:
    def __init__(self, db_repository: DatabaseRepository):
        self.db_repository = db_repository
        self.logger = logging.getLogger('item_service')
        
    async def fetch_items(self, proxies: List[str]):
        total_listings_queue = asyncio.Queue()
        processing_queue = asyncio.Queue()
        fetch_start = datetime.now().timestamp()
        
        # Queue all items for total count fetching
        for item in ITEMS + COURIERS:
            await total_listings_queue.put(item)
            
        # Add stop signals for total listings fetchers
        for _ in proxies:
            await total_listings_queue.put(None)
            
        # Create and start total listings fetchers
        total_fetchers = [
            self._total_listings_fetcher(total_listings_queue, processing_queue, proxy)
            for proxy in proxies
        ]
        
        # Wait for all total counts to be fetched and split into batches
        await asyncio.gather(*total_fetchers)
        
        # Add stop signals for processors after all listings are queued
        for _ in proxies:
            await processing_queue.put(None)
            
        # Create and start item processors
        processors = [
            self._item_processor(processing_queue, proxy)
            for proxy in proxies
        ]
        
        # Wait for all processors to complete
        await asyncio.gather(*processors)
        
        fetch_end = datetime.now().timestamp()
        
        # Save fetch timestamps to the database
        self.db_repository.save_fetch_timestamps(fetch_start, fetch_end)
        
    async def _total_listings_fetcher(self, input_queue: asyncio.Queue, output_queue: asyncio.Queue, proxy: str):
        worker_logger = WorkerLogger('item_service', proxy)
        client = SteamPublicClient(proxy=proxy, currency=Currency.KZT)
        
        try:
            while True:
                item = await input_queue.get()
                if item is None:
                    break
                    
                worker_logger.set_item(item)
                # Added retry mechanism similar to gem_service for proxy failures
                retries = 0
                max_retries = 3
                success = False
                while retries < max_retries and not success:
                    try:
                        # Add delay between requests
                        await asyncio.sleep(1)
                        
                        worker_logger.info(f"Fetching total listings for item: {item}")
                        total_count = await self._fetch_total_listings(client, item)
                        worker_logger.info(f"Found {total_count} total listings")
                        
                        if total_count > 0:
                            # Split into batches of 100 (matching test.py)
                            for start in range(0, total_count, 100):
                                batch = {
                                    'item': item,
                                    'start': start,
                                    'count': min(100, total_count - start)
                                }
                                await output_queue.put(batch)
                        # Mark as successful even if total_count is 0 (could be a valid result)
                        success = True
                    except Exception as e:
                        retries += 1
                        worker_logger.error(f"Error fetching total listings for item: {item} on attempt {retries}: {e}", exc_info=True)
                        if retries < max_retries:
                            backoff = (2 ** (retries - 1)) * 2  # exponential backoff
                            worker_logger.info(f"Waiting {backoff}s before retrying item: {item}")
                            await asyncio.sleep(backoff)
                        else:
                            worker_logger.error(f"Max retries reached for item: {item}, requeuing task.")
                            # Requeue the task for other proxies
                            await input_queue.put(item)
                    finally:
                        # This line remains untouched: mark the original task as done
                        input_queue.task_done()
                        
        except Exception as e:
            worker_logger.error(f"Total listings fetcher encountered an error: {e}", exc_info=True)
            
        finally:
            await client.session.close()
            worker_logger.info("Total listings fetcher finished")
            
    async def _item_processor(self, listings_queue: asyncio.Queue, proxy: str):
        worker_logger = WorkerLogger('item_service', proxy)
        client = SteamPublicClient(proxy=proxy, currency=Currency.KZT)
        listings_processed = 0

        try:
            while True:
                batch = await listings_queue.get()
                if batch is None:
                    break

                worker_logger.set_item(batch['item'])
                # Added retry mechanism similar to gem_service for proxy failures in processing a batch
                retries = 0
                max_retries = 3
                success = False
                df = None
                while retries < max_retries and not success:
                    try:
                        listings = await self._fetch_listings_for_item_range(client, batch['item'], batch['start'])
                        # Process listings using the existing parser
                        df, parsed_ids = parse_market_listings(listings)
                        success = True
                    except Exception as e:
                        retries += 1
                        worker_logger.error(f"Error fetching listings for item '{batch['item']}' at start {batch['start']} on attempt {retries}: {e}", exc_info=True)
                        if retries < max_retries:
                            backoff = (2 ** (retries - 1)) * 2  # exponential backoff
                            worker_logger.info(f"Waiting {backoff}s before retrying batch for item: {batch['item']}")
                            await asyncio.sleep(backoff)
                        else:
                            worker_logger.error(f"Max retries reached for batch of item: {batch['item']}, requeuing batch.")
                            # Requeue the failed batch for other proxy workers to try
                            await listings_queue.put(batch)
                            break

                if not success:
                    listings_queue.task_done()
                    continue

                try:
                    if not df.empty:
                        worker_logger.info(f"Found {len(df)} items with gems")
                        for _, row in df.iterrows():
                            try:
                                current_time = datetime.now().timestamp()
                                worker_logger.debug(f"Raw row data: {row.to_dict()}")

                                ethereal_gem = str(row["Ethereal Gem"]) if pd.notna(row["Ethereal Gem"]) else None
                                prismatic_gem = str(row["Prismatic Gem"]) if pd.notna(row["Prismatic Gem"]) else None

                                if ethereal_gem and ethereal_gem not in ALLOWED_GEMS_ETHEREAL:
                                    worker_logger.warning(f"Invalid ethereal gem name: {ethereal_gem}")
                                    ethereal_gem = None
                                if prismatic_gem and prismatic_gem not in ALLOWED_GEMS_PRISMATIC:
                                    worker_logger.warning(f"Invalid prismatic gem name: {prismatic_gem}")
                                    prismatic_gem = None

                                item = Item(
                                    id=row["ID"],
                                    name=row["Item Description"],
                                    price=float(row["Price"]),
                                    ethereal_gem=ethereal_gem,
                                    prismatic_gem=prismatic_gem,
                                    timestamp=current_time
                                )
                                self.db_repository.save_item(item)
                            except Exception as e:
                                worker_logger.error(f"Error saving item: {e}", exc_info=True)
                    else:
                        worker_logger.info("No items with gems found in this batch.")

                    # New: Clean up raw listings that weren't successfully parsed
                    if listings:  # Only if we have listings
                        fetched_ids = {listing.id for listing in listings}
                        ids_to_remove = fetched_ids - parsed_ids
                        if ids_to_remove:
                            worker_logger.info(f"Cleaning up {len(ids_to_remove)} unparsed raw listings")
                            self.db_repository.remove_raw_listings(ids_to_remove)

                    listings_processed += batch['count']
                    if listings_processed >= settings.LISTINGS_BEFORE_BATCH_DELAY:
                        worker_logger.debug(f"Sleeping for {settings.BATCH_DELAY}s after processing {listings_processed} listings")
                        await asyncio.sleep(settings.BATCH_DELAY)
                        listings_processed = 0

                except Exception as e:
                    worker_logger.error(f"Error processing batch: {e}", exc_info=True)

                finally:
                    listings_queue.task_done()

        except Exception as e:
            worker_logger.error(f"Worker encountered an error: {e}", exc_info=True)

        finally:
            await client.session.close()
            worker_logger.info("Item processor finished")
        
    async def _fetch_total_listings(self, client: SteamPublicClient, item: str) -> int:
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                _, total_count, _ = await client.get_item_listings(
                    item,
                    App.DOTA2,
                    count=settings.LISTINGS_PER_REQUEST, #15->100
                    start=0
                )
                
                if total_count == 0:
                    self.logger.warning(f"Zero listings returned for '{item}'. This may indicate an API issue.")
                else:
                    self.logger.info(f"Successfully fetched {total_count} total listings for '{item}'")
                
                return total_count

            except Exception as e:
                if attempt < max_retries - 1:
                    self.logger.warning(f"Attempt {attempt + 1} failed to fetch listings for '{item}': {e}")
                    await asyncio.sleep(retry_delay * (attempt + 1))
                else:
                    self.logger.error(f"Failed after {max_retries} attempts to fetch listings for '{item}': {e}")
                    return 0

        return 0
        
    async def _fetch_listings_for_item_range(self, client: SteamPublicClient, item: str, start: int):
        try:
            listings, _, _ = await client.get_item_listings(
                item,
                App.DOTA2,
                count=settings.LISTINGS_PER_REQUEST,
                start=start
            )
            
            # Store each raw listing right after fetching
            current_time = datetime.now().timestamp()
            for listing in listings:
                try:
                    self.db_repository.save_raw_listing(listing.id, listing, current_time)
                except Exception as e:
                    logging.error(f"Failed to store raw listing {listing.id}: {e}")
            
            return listings
        except Exception as e:
            logging.error(f"Error fetching listings for item '{item}' at start {start}: {e}")
            return []
 