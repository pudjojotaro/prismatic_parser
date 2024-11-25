import sqlite3
from ..config.settings import settings

def init_db():
    conn = sqlite3.connect(settings.DATABASE_PATH)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS items (
        id TEXT PRIMARY KEY,
        name TEXT,
        price REAL,
        ethereal_gem TEXT,
        prismatic_gem TEXT,
        timestamp REAL
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS gems (
        name TEXT PRIMARY KEY,
        buy_orders TEXT NOT NULL,
        buy_order_length INTEGER NOT NULL,
        timestamp REAL NOT NULL
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS comparisons (
        item_id TEXT PRIMARY KEY,
        item_price REAL,
        is_profitable BOOLEAN,
        timestamp REAL,
        prismatic_gem_price REAL,
        ethereal_gem_price REAL,
        combined_gem_price REAL,
        expected_profit REAL
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fetch_timestamps (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fetch_start_timestamp REAL,
        fetch_end_timestamp REAL
    )
    """)
    conn.commit()
    conn.close()
