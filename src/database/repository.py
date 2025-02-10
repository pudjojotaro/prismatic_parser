import sqlite3
from contextlib import contextmanager
from typing import Optional, List, Tuple
from ..config.settings import settings
from ..models.item import Item
from ..models.gem import Gem
from ..models.comparison import Comparison
import logging
import pickle

class DatabaseRepository:
    def __init__(self):
        self.db_path = settings.DATABASE_PATH
        self.logger = logging.getLogger('database')
        
    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()
            
    def save_item(self, item: Item) -> None:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO items
                (id, name, price, ethereal_gem, prismatic_gem, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                item.id, item.name, item.price, 
                item.ethereal_gem, item.prismatic_gem, item.timestamp
            ))
            conn.commit()
            
    def get_item(self, item_id: str) -> Optional[Item]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, name, price, ethereal_gem, prismatic_gem, timestamp
                FROM items
                WHERE id = ?
            """, (item_id,))
            row = cursor.fetchone()
            if row:
                return Item(*row)
            return None
        
    def get_all_items(self) -> List[Item]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, name, price, ethereal_gem, prismatic_gem, timestamp
                FROM items
            """)
            rows = cursor.fetchall()
            return [Item(*row) for row in rows]
        
    def save_gem(self, gem: Gem) -> None:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO gems
                (name, buy_orders, buy_order_length, timestamp)
                VALUES (?, ?, ?, ?)
            """, (
                gem.name, gem.buy_orders, gem.buy_order_length, gem.timestamp
            ))
            conn.commit()
            
    def get_gem(self, gem_name: str) -> Optional[Gem]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT name, buy_orders, buy_order_length, timestamp
                FROM gems
                WHERE name = ?
            """, (gem_name,))
            row = cursor.fetchone()
            if row:
                return Gem(*row)
            return None
        
    def save_comparison(self, comparison: Comparison) -> None:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO comparisons
                (item_id, item_price, is_profitable, timestamp, 
                 prismatic_gem_price, ethereal_gem_price, combined_gem_price, expected_profit)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                comparison.item_id, comparison.item_price, comparison.is_profitable, comparison.timestamp,
                comparison.prismatic_gem_price, comparison.ethereal_gem_price,
                comparison.combined_gem_price, comparison.expected_profit
            ))
            conn.commit()
            
    def get_comparison(self, item_id: str) -> Optional[Comparison]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT item_id, item_price, is_profitable, timestamp,
                       prismatic_gem_price, ethereal_gem_price, combined_gem_price, expected_profit
                FROM comparisons
                WHERE item_id = ?
            """, (item_id,))
            row = cursor.fetchone()
            if row:
                return Comparison(*row)
            return None
        
    def save_fetch_timestamps(self, fetch_start: float, fetch_end: float) -> None:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO fetch_timestamps (fetch_start_timestamp, fetch_end_timestamp)
                VALUES (?, ?)
            """, (fetch_start, fetch_end))
            conn.commit()
            
    def get_last_fetch_timestamps(self) -> Tuple[Optional[float], Optional[float]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT fetch_start_timestamp, fetch_end_timestamp
                FROM fetch_timestamps
                ORDER BY id DESC
                LIMIT 1
            """)
            row = cursor.fetchone()
            if row:
                return row
            return None, None
        
    def get_items_in_timerange(self, start_time: float, end_time: float) -> List[Item]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, name, price, ethereal_gem, prismatic_gem, timestamp
                FROM items
                WHERE timestamp BETWEEN ? AND ?
            """, (start_time, end_time))
            rows = cursor.fetchall()
            return [Item(*row) for row in rows]

    def save_raw_listing(self, listing_id: str, listing_obj, timestamp: float) -> None:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            pickled_data = pickle.dumps(listing_obj)
            cursor.execute("""
                INSERT OR REPLACE INTO raw_listings
                (id, listing_data, fetch_timestamp)
                VALUES (?, ?, ?)
            """, (listing_id, pickled_data, timestamp))
            conn.commit()
    
    def get_raw_listing(self, listing_id: str):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT listing_data
                FROM raw_listings
                WHERE id = ?
            """, (listing_id,))
            row = cursor.fetchone()
            if row:
                return pickle.loads(row[0])
            return None

    def remove_raw_listings(self, listing_ids: set) -> None:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            # SQLite doesn't support multiple values in IN clause directly
            # so we need to create the placeholders
            placeholders = ','.join('?' * len(listing_ids))
            cursor.execute(f"""
                DELETE FROM raw_listings
                WHERE id IN ({placeholders})
            """, tuple(listing_ids))
            conn.commit()
