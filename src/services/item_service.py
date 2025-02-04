import asyncio
import logging
from typing import List
from aiosteampy import SteamPublicClient, App
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
        client = SteamPublicClient(proxy=proxy)
        
        try:
            while True:
                item = await input_queue.get()
                if item is None:
                    break
                    
                worker_logger.set_item(item)
                try:
                    worker_logger.info(f"Fetching total listings for item: {item}")
                    total_count = await self._fetch_total_listings(client, item)
                    worker_logger.info(f"Found {total_count} total listings")
                    
                    # Split into batches of LISTINGS_PER_REQUEST
                    for start in range(0, total_count, settings.LISTINGS_PER_REQUEST):
                        batch = {
                            'item': item,
                            'start': start,
                            'count': min(settings.LISTINGS_PER_REQUEST, total_count - start)
                        }
                        await output_queue.put(batch)
                            
                except Exception as e:
                    worker_logger.error(f"Error fetching total listings: {e}", exc_info=True)
                finally:
                    input_queue.task_done()
                    
        except Exception as e:
            worker_logger.error(f"Total listings fetcher encountered an error: {e}", exc_info=True)
            
        finally:
            await client.session.close()
            worker_logger.info("Total listings fetcher finished")
            
    async def _item_processor(self, listings_queue: asyncio.Queue, proxy: str):
        worker_logger = WorkerLogger('item_service', proxy)
        client = SteamPublicClient(proxy=proxy)
        listings_processed = 0

        try:
            while True:
                batch = await listings_queue.get()
                if batch is None:
                    break

                worker_logger.set_item(batch['item'])
                try:
                    listings = await self._fetch_listings_for_item_range(
                        client, 
                        batch['item'], 
                        batch['start']
                    )

                    df = parse_market_listings(listings)
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
        
        # Add debug logging for the exact item string
        self.logger.debug(f"Item string: '{item}', length: {len(item)}, bytes: {item.encode('utf-8')}")
        
        for attempt in range(max_retries):
            try:
                _, total_count, _ = await client.get_item_listings(
                    item.strip(),  # Ensure no whitespace
                    App.DOTA2,
                    count=settings.LISTINGS_PER_REQUEST,
                    start=0
                )
                self.logger.info(f"Total listings found: {total_count}")
                if total_count == 0:
                    self.logger.warning(f"Zero listings returned for '{item}'. This may indicate an API issue.")
                else:
                    self.logger.info(f"Successfully fetched {total_count} total listings for '{item}'")
                
                return total_count

            except Exception as e:
                if attempt < max_retries - 1:
                    self.logger.warning(f"Attempt {attempt + 1} failed to fetch listings for '{item}': {e}")
                    await asyncio.sleep(retry_delay * (attempt + 1))  # Exponential backoff
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
            return listings
        except Exception as e:
            logging.error(f"Error fetching listings for item '{item}' at start {start}: {e}")
            return []
 