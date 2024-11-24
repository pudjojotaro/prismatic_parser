import asyncio
import json
from pathlib import Path
from aiosteampy import App, AppContext # type: ignore
import pandas as pd # type: ignore
from bs4 import BeautifulSoup # type: ignore
from aiosteampy import SteamClient, Currency, AppContext # type: ignore
from aiosteampy.helpers import restore_from_cookies # type: ignore
from aiosteampy.utils import get_jsonable_cookies # type: ignore 
from aiosteampy import ResourceNotModified # type: ignore
import random
from aiosteampy import SteamClient # type: ignore
import itertools
from aiosteampy import SteamPublicClient # type: ignore
import time
from collections import defaultdict
import logging
import sqlite3
from datetime import datetime, timedelta
import sys
import os
from telegram_alert_bot import TelegramAlertBot
from dotenv import load_dotenv
from proxy_api import ProxyAPI # type: ignore
import math
import requests

# Global variable for the bot instance
telegram_bot = None

current_dir = os.path.dirname(os.path.abspath(__file__))
projects_dir = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.append(projects_dir)


profitable_item_found = False

def load_constants():
    """
    Load all constant data from JSON files in the data directory.
    Returns tuple of (items, couriers, allowed_gems_prismatic, allowed_gems_ethereal)
    """
    data_dir = Path("data")
    
    try:
        # Load items
        with open(data_dir / "items.json", "r") as f:
            ITEMS = json.load(f)
            
        # Load couriers
        with open(data_dir / "couriers.json", "r") as f:
            COURIERS = json.load(f)
            
        # Load gems
        with open(data_dir / "gems_prismatic.json", "r") as f:
            ALLOWED_GEMS_PRISMATIC = json.load(f)
            
        with open(data_dir / "gems_ethereal.json", "r") as f:
            ALLOWED_GEMS_ETHEREAL = json.load(f)
            
        logging.info("Successfully loaded all constant data")
        return ITEMS, COURIERS, ALLOWED_GEMS_PRISMATIC, ALLOWED_GEMS_ETHEREAL
        
    except FileNotFoundError as e:
        logging.error(f"Missing data file: {e.filename}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in data file: {e}")
        raise



DATABASE_PATH = "data/items_data.db"



# Configure logging
os.makedirs("./logs", exist_ok=True)  # Create logs directory if it doesn't exist
logging.basicConfig(
    filename="./logs/logs.txt",
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    filemode="w"  # Overwrites the log file on each run
)

def init_db():
    """
    Initializes the database by creating necessary tables if they do not already exist.

    Tables:
        - items: Stores data about items/couriers including price, gems, and timestamps.
        - gems: Stores data about gems, including buy orders and timestamps.
        - comparisons: Stores comparison data for items, including profitability and expected profit.
        - fetch_timestamps: Stores the timestamps of fetch start and end.

    Columns:
        - items:
            - id: Unique identifier for the item (Primary Key).
            - name: Name of the item.
            - price: Price of the item.
            - ethereal_gem: Associated ethereal gem.
            - prismatic_gem: Associated prismatic gem.
            - timestamp: Timestamp of the last update.
        - gems:
            - name: Name of the gem (Primary Key).
            - buy_orders: JSON string containing the buy orders.
            - buy_order_length: Number of buy orders.
            - timestamp: Timestamp of the last update.
        - comparisons:
            - item_id: Unique identifier for the item (Primary Key).
            - item_price: Price of the item.
            - is_profitable: Whether the item is profitable (Boolean).
            - timestamp: Timestamp of the last update.
            - prismatic_gem_price: Price of the associated prismatic gem.
            - ethereal_gem_price: Price of the associated ethereal gem.
            - combined_gem_price: Combined price of both gems.
            - expected_profit: Calculated expected profit for the item.
        - fetch_timestamps:
            - id: Unique identifier for each fetch record (Primary Key).
            - fetch_start_timestamp: The timestamp when the fetch started.
            - fetch_end_timestamp: The timestamp when the fetch ended

    Commits the changes to the database and closes the connection.
    """
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    # Create items table for storing item/courier data
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

    # Create gems table for storing gem data
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS gems (
        name TEXT PRIMARY KEY,
        buy_orders TEXT,  -- JSON string of buy orders
        buy_order_length INTEGER,
        timestamp REAL
    )
    """)

    # Create a table for comparisons
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS comparisons (
        item_id TEXT PRIMARY KEY,  -- Ensure each item has a unique entry
        item_price REAL,
        is_profitable BOOLEAN,
        timestamp REAL,
        prismatic_gem_price REAL,
        ethereal_gem_price REAL,
        combined_gem_price REAL,
        expected_profit REAL  -- Add the expected profit column
    )
    """)

    # Create fetch_timestamps table for storing fetch start and end times
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fetch_timestamps (
        id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Auto-incrementing ID for each fetch record
        fetch_start_timestamp REAL,           -- The timestamp when the fetch started
        fetch_end_timestamp REAL              -- The timestamp when the fetch ended
    )
    """)

    conn.commit()
    conn.close()

def save_fetch_timestamps(fetch_start, fetch_end):
    """
    Saves the fetch start and end timestamps into the fetch_timestamps table.

    Parameters:
        fetch_start (float): The timestamp when the fetch started (Unix timestamp).
        fetch_end (float): The timestamp when the fetch ended (Unix timestamp).
    """
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    try:
        # Insert the timestamps into the fetch_timestamps table
        cursor.execute("""
        INSERT INTO fetch_timestamps (fetch_start_timestamp, fetch_end_timestamp)
        VALUES (?, ?)
        """, (fetch_start, fetch_end))

        conn.commit()
        logging.info(f"Fetch timestamps saved: start={fetch_start}, end={fetch_end}")
    except sqlite3.Error as e:
        logging.error(f"Error saving fetch timestamps: {e}")
    finally:
        conn.close()

def get_last_fetch_timestamps():
    """
    Retrieves the most recent fetch start and end timestamps from the fetch_timestamps table.

    Returns:
        tuple: A tuple containing the last fetch start timestamp and fetch end timestamp as floats.
               Returns (None, None) if no timestamps are found.
    """
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    try:
        # Query to retrieve the latest fetch timestamps
        cursor.execute("""
        SELECT fetch_start_timestamp, fetch_end_timestamp
        FROM fetch_timestamps
        ORDER BY id DESC
        LIMIT 1
        """)
        result = cursor.fetchone()

        if result:
            fetch_start, fetch_end = result
            logging.info(f"Last fetch timestamps retrieved: start={fetch_start}, end={fetch_end}")
            return fetch_start, fetch_end
        else:
            logging.info("No fetch timestamps found in the database.")
            return None, None
    except sqlite3.Error as e:
        logging.error(f"Error retrieving last fetch timestamps: {e}")
        return None, None
    finally:
        conn.close()

async def send_no_profitable_items_alert(fetch_start=None, fetch_end=None):
    """
    Sends an alert that no profitable items were found between the last fetch start and end timestamps.
    """
    try:
        if not fetch_start or not fetch_end:
            logging.error("No fetch timestamps available. Unable to send alert.")
            return

        # Convert timestamps to human-readable time
        fetch_start_time = datetime.fromtimestamp(fetch_start).strftime('%Y-%m-%d %H:%M:%S')
        fetch_end_time = datetime.fromtimestamp(fetch_end).strftime('%Y-%m-%d %H:%M:%S')

        # Construct the alert message
        message = (
            f"No profitable items found between {fetch_start_time} and {fetch_end_time}."
        )

        logging.info(f"Sending alert: {message}")
        await telegram_bot.event_trigger(message, "Prismatic_Parser_Bot")
        
    except Exception as e:
        logging.error(f"Error in send_no_profitable_items_alert: {e}")


def clear_all_tables():
    """
    Clear all rows from the `items`, `gems`, and `comparisons` tables in the database.
    """
    try:
        # Connect to the database
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()

        # List of tables to clear
        tables = ["items", "gems", "comparisons"]

        # Iterate through tables and delete all rows
        for table in tables:
            cursor.execute(f"DELETE FROM {table}")
            print(f"Cleared all entries from the `{table}` table.")

        # Commit changes and close the connection
        conn.commit()
        conn.close()
        print("All tables have been successfully cleared.")

    except sqlite3.OperationalError as e:
        print(f"Error clearing tables: {e}")



# Function to load proxies from file
def load_proxies(proxy_type="items"):
    """
    Loads a list of proxies from the appropriate file based on the specified type.

    Parameters:
        proxy_type (str): The type of proxies to load ('items' or 'gems').

    Returns:
        list: A list of proxies as strings.

    Raises:
        ValueError: If an invalid proxy type is specified.
    """
    if proxy_type == "items":
        proxy_file = Path("./proxies/proxies_items.txt")
    elif proxy_type == "gems":
        proxy_file = Path("./proxies/proxies_gems.txt")
    else:
        raise ValueError("Invalid proxy type. Use 'items' or 'gems'.")

    # Read proxies from the specified file
    with open(proxy_file, "r") as f:
        proxies = f.read().splitlines()
    return proxies


# Load gems data from JSON
def load_gems_data():
    """
    Loads gem data from JSON files for both ethereal and prismatic gems.

    Returns:
        dict: A dictionary combining data from both gem types.
    """
    with open("./python_helpers/gems_ethereal_with_ID.json", "r") as ethereal_file, open("./python_helpers/gems_prismatic_with_ID.json", "r") as prismatic_file:
        ethereal_gems = json.load(ethereal_file)
        prismatic_gems = json.load(prismatic_file)
    
    # Combine the data from both files into a single dictionary
    return {**ethereal_gems, **prismatic_gems}


def process_histogram(histogram):
    """
    Processes a histogram to extract and adjust buy order data.

    Parameters:
        histogram (object): The histogram object containing buy order data.

    Returns:
        dict: A dictionary containing processed buy orders and the total buy order length.

    Functionality:
        - Extracts and adjusts cumulative quantities in buy orders to represent incremental quantities.
        - Calculates the total quantity of buy orders after adjustments.

    Example:
        Input histogram.buy_order_graph: [{'price': 100, 'quantity': 10}, ...]
        Output: {'buy_orders': [[1.00, 10], ...], 'buy_order_length': 10}
    """
    # Initialize list to store processed buy orders
    buy_orders = []

    # Process the buy order graph if it exists and is in the expected format
    if hasattr(histogram, 'buy_order_graph') and isinstance(histogram.buy_order_graph, list):
        # Convert buy orders to list format for easier manipulation
        buy_orders = [[entry.price / 100, entry.quantity] for entry in histogram.buy_order_graph]

        # Adjust buy order quantities to represent incremental quantities
        for i in range(len(buy_orders) - 1, 0, -1):
            current_quantity = buy_orders[i][1]
            previous_quantity = buy_orders[i - 1][1]
            reduced_quantity = current_quantity - previous_quantity
            buy_orders[i][1] = reduced_quantity

    # Calculate total buy order quantity after adjustments
    buy_order_length = sum(order[1] for order in buy_orders if order[1] > 0)

    # Return processed data with only buy orders
    processed_data = {
        "buy_orders": buy_orders,
        "buy_order_length": buy_order_length,
    }
    return processed_data


# Worker Function to Fetch Order Histogram and Activity
async def fetch_gem_data_worker(gem_task_queue, proxy):
    """
    Worker function to process tasks for fetching gem data.

    Parameters:
        gem_task_queue (asyncio.Queue): The queue containing gem tasks to process.
        proxy (str): The proxy to use for making requests.

    Functionality:
        - Fetches order histogram and activity for each gem in the task queue.
        - Processes and parses the histogram data.
        - Updates the gem data in the database.
        - Enforces a delay after processing two items to avoid rate-limiting.
    """
    client = SteamPublicClient(proxy=proxy)
    items_processed = 0  # Counter to track items processed per worker

    try:
        while not gem_task_queue.empty():
            # Get the next task from the queue
            gem_name, item_name_id = await gem_task_queue.get()
            print(f"[Gem Worker {proxy}] Fetching data for gem: {gem_name}")

            try:
                # Fetch histogram and activity
                histogram = await client.get_item_orders_histogram(item_name_id)
                
                # Check if histogram is a tuple and extract if necessary
                if isinstance(histogram, tuple):
                    histogram = histogram[0]  # Assuming the first item is the desired data

                # Parse buy order data from histogram
                parsed_data = process_histogram(histogram)

                if parsed_data is not None:
                    # Update the gem data in the database
                    update_gem_in_db(gem_name, parsed_data)
                    print(f"[Gem Worker {proxy}] Buy order data fetched and stored for gem: {gem_name}")

            except Exception as e:
                # Log any errors encountered during processing
                print(f"[Gem Worker {proxy}] Error fetching data for gem '{gem_name}': {e}")

            # Mark the task as done in the queue
            gem_task_queue.task_done()

            # Increment items processed counter
            items_processed += 1

            # Enforce a delay after processing two items to avoid rate-limiting
            if items_processed >= 3:
                print(f"[Gem Worker {proxy}] Pausing for {REQUEST_DELAY} seconds after processing 3 items.")
                await asyncio.sleep(REQUEST_DELAY)
                items_processed = 0  # Reset the counter
    finally:
        # Close the client session when the worker exits
        await client.session.close()


# Function to parse market listings
def parse_market_listings(market_listings):
    """
    Parses the market listings to extract relevant data.

    Parameters:
        market_listings (list): A list of market listing objects.

    Returns:
        pd.DataFrame: A DataFrame containing parsed listing data.

    Functionality:
        - Extracts details such as ID, price, item description, type, and gems.
        - Identifies and processes gem data for items in the COURIERS list.
        - Filters prismatic and ethereal gems based on allowed gem lists.
        - Includes items only if at least one valid gem is present.
    """
    data = []
    
    # Loop over each listing to extract data
    for listing in market_listings:
        # Extract the item's description and initialize listing data
        item_description = listing.item.description.market_name
        listing_data = {
            "ID": listing.id,
            "Price": listing.converted_price / 100 + listing.converted_fee/100,  # Convert price to float (divide by 100)
            "Item Description": item_description,
            "Type": listing.item.description.type,
            "Ethereal Gem": None,
            "Prismatic Gem": None,
        }

        # Check if the item is in the COURIERS list
        if item_description in COURIERS:
            ethereal_gem = None
            prismatic_gem = None
            
            # Extract and filter gem data specific to couriers
            for desc in listing.item.description.descriptions:
                if "Gem" in desc.value:
                    # Use BeautifulSoup to clean HTML and extract text
                    soup = BeautifulSoup(desc.value, 'html.parser')
                    gem_text = soup.get_text(strip=True).replace("Empty Socket", "").strip()

                    # Identify and validate prismatic gems
                    if "Prismatic Gem" in gem_text:
                        matching_prismatic_gem = next((allowed for allowed in ALLOWED_GEMS_PRISMATIC if allowed in gem_text), None)
                        if matching_prismatic_gem:
                            prismatic_gem = matching_prismatic_gem

                    # Identify and validate ethereal gems
                    if "Ethereal Gem" in gem_text:
                        matching_ethereal_gem = next((allowed for allowed in ALLOWED_GEMS_ETHEREAL if allowed in gem_text), None)
                        if matching_ethereal_gem:
                            ethereal_gem = matching_ethereal_gem
            
            # Update the listing data with filtered gems
            listing_data["Ethereal Gem"] = ethereal_gem
            listing_data["Prismatic Gem"] = prismatic_gem
            print(prismatic_gem, ethereal_gem)

        else:
            # Process non-courier items with allowed_gems filtering
            for desc in listing.item.description.descriptions:
                if "Gem" in desc.value:
                    # Use BeautifulSoup to clean HTML and extract text
                    soup = BeautifulSoup(desc.value, 'html.parser')
                    gem_text = soup.get_text(strip=True).replace("Empty Socket", "").strip()

                    # Skip items matching specific exclusion conditions
                    if (
                        "Swine of the Sunken Galley" in item_description and "Explosive Burst" in gem_text
                    ) or (
                        "Fractal Horns of Inner Abysm" in item_description and "Reflection's Shade" in gem_text
                    ):
                        listing_data["Prismatic Gem"] = None
                    else:
                        # Filter and validate prismatic gems for non-courier items
                        matching_gem = next((allowed for allowed in ALLOWED_GEMS_PRISMATIC if allowed in gem_text), None)
                        if matching_gem:
                            listing_data["Prismatic Gem"] = matching_gem

                    break  # Exit the loop after processing the gem

        # Include listing in the result only if at least one valid gem is present
        if listing_data["Ethereal Gem"] or listing_data["Prismatic Gem"]:
            data.append(listing_data)
    
    # Convert the data into a DataFrame for easier processing
    df = pd.DataFrame(data)
    return df




import asyncio
import time
from collections import defaultdict


async def fetch_total_listings(client, item):
    """
    Fetches the total number of listings for a given item.

    Parameters:
        client (SteamPublicClient): The Steam client used for fetching listings.
        item (str): The name or ID of the item to fetch listings for.

    Returns:
        int: The total number of listings for the specified item.
             Returns 0 if an error occurs.
    """
    try:
        # Fetch the first batch of listings to determine the total count
        _, total_count, _ = await client.get_item_listings(
            item,
            App.DOTA2,
            count=LISTINGS_PER_REQUEST,  # Limit the number of listings per request
            start=0  # Start from the first listing
        )
        print(f"Total listings for '{item}': {total_count}")
        return total_count
    except Exception as e:
        # Handle and log errors
        print(f"Error fetching total listings for '{item}': {e}")
        return 0



def update_gem_in_db(gem_name, buy_orders):
    """
    Updates or inserts gem data into the gems database.

    Parameters:
        gem_name (str): The name of the gem.
        buy_orders (list): A list of buy orders for the gem.
    """
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    # Use INSERT OR REPLACE to update existing entries or insert new ones
    cursor.execute("""
    INSERT OR REPLACE INTO gems (name, buy_orders, buy_order_length, timestamp)
    VALUES (?, ?, ?, ?)
    """, (
        gem_name,
        json.dumps(buy_orders),  # Convert buy_orders to a JSON string for storage
        len(buy_orders),  # Store the number of buy orders
        time.time()  # Use the current time as the timestamp
    ))

    conn.commit()
    conn.close()


def fetch_gem_from_db(gem_name):
    """
    Fetches a gem's details from the database by its name.

    Parameters:
        gem_name (str): The name of the gem to fetch.

    Returns:
        dict: A dictionary containing the gem's details if found, or None if not found.
    """
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    # Fetch the gem by name
    cursor.execute("SELECT * FROM gems WHERE name = ?", (gem_name,))
    gem = cursor.fetchone()
    conn.close()

    # If the gem exists, convert it into a dictionary
    if gem:
        return {
            "name": gem[0],
            "buy_orders": json.loads(gem[1]),  # Parse the JSON string back into a Python list
            "buy_order_length": gem[2],
            "timestamp": gem[3]
        }
    return None


def fetch_all_gems_from_db():
    """
    Fetches all gems from the gems database.

    Returns:
        list: A list of dictionaries, each containing a gem's details.
    """
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    # Fetch all rows from the gems table
    cursor.execute("SELECT * FROM gems")
    all_gems = cursor.fetchall()
    conn.close()

    # Convert each row into a dictionary
    return [
        {
            "name": gem[0],
            "buy_orders": json.loads(gem[1]),  # Parse the JSON string back into a Python list
            "buy_order_length": gem[2],
            "timestamp": gem[3]
        } for gem in all_gems
    ]


def update_item_in_db(item_id, name, price, ethereal_gem, prismatic_gem):
    """
    Updates or inserts an item into the items database.

    Parameters:
        item_id (str): The unique ID of the item.
        name (str): The name of the item.
        price (float): The price of the item.
        ethereal_gem (str): The name of the ethereal gem associated with the item.
        prismatic_gem (str): The name of the prismatic gem associated with the item.
    """
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    # Use INSERT OR REPLACE to update existing entries or insert new ones
    cursor.execute("""
    INSERT OR REPLACE INTO items (id, name, price, ethereal_gem, prismatic_gem, timestamp)
    VALUES (?, ?, ?, ?, ?, ?)
    """, (
        item_id, name, price, ethereal_gem, prismatic_gem, time.time(),  # Use the current time as the timestamp
    ))

    conn.commit()
    conn.close()


def fetch_item_from_db(item_id):
    """
    Fetches an item's details from the database by its ID.

    Parameters:
        item_id (str): The unique ID of the item to fetch.

    Returns:
        dict: A dictionary containing the item's details if found, or None if not found.
    """
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    # Fetch the item by ID
    cursor.execute("SELECT * FROM items WHERE id = ?", (item_id,))
    item = cursor.fetchone()
    conn.close()

    # If the item exists, convert it into a dictionary
    if item:
        return {
            "id": item[0],
            "name": item[1],
            "price": item[2],
            "ethereal_gem": item[3],
            "prismatic_gem": item[4],
            "timestamp": item[5]
        }
    return None



def fetch_all_items_from_db():
    """
    Fetches all items from the items database.

    Returns:
        list: A list of dictionaries, each containing an item's details.
    """
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    # Fetch all rows from the items table
    cursor.execute("SELECT * FROM items")
    all_items = cursor.fetchall()
    conn.close()

    # Convert each row into a dictionary
    return [
        {
            "id": item[0],
            "name": item[1],
            "price": item[2],
            "ethereal_gem": item[3],
            "prismatic_gem": item[4],
            "timestamp": item[5]
        } for item in all_items
    ]


# Update `real_time_item_storage` with listings data
def update_db_item_storage(item, listings_df):
    """
    Update the real-time item storage with new listings data for a specific item.
    Only stores items with valid gem data.
    """
    for _, row in listings_df.iterrows():
        item_id = row['ID']
        
        # Extract Ethereal and Prismatic Gem data
        ethereal_gem = row['Ethereal Gem'] if pd.notna(row['Ethereal Gem']) else ""
        prismatic_gem = row['Prismatic Gem'] if pd.notna(row['Prismatic Gem']) else ""
        
        item_data = {
            "id": item_id,
            "price": row['Price'],
            "ethereal_gem": ethereal_gem,
            "prismatic_gem": prismatic_gem,
            #"gem": gem_info,  # Optional combined field
            "timestamp": time.time(),
            "item_description": item  # Add item description for tracking
        }

        """
        # Store item data in the real-time storage by item ID
        real_time_item_storage[item_id] = item_data
        """

        update_item_in_db(item_id, item, row['Price'], ethereal_gem, prismatic_gem)

        # Log the addition to real_time_item_storage
        logging.info(f"Added item to real_time_item_storage: {item_data}")


# Function to fetch listings for a specific range within an item
async def fetch_listings_for_item_range(client, item, start, proxy, last_modified=None):
    """
    Fetches market listings for a specific item within a given range.

    Parameters:
        client (SteamPublicClient): The Steam client used for fetching listings.
        item (str): The name or ID of the item to fetch listings for.
        start (int): The starting offset for the listings.
        proxy (str): The proxy being used for the request.
        last_modified (datetime, optional): The timestamp to use for If-Modified-Since header.

    Returns:
        tuple: A tuple containing the fetched listings and the new last modified timestamp.
               If an error occurs, returns (None, None).
    """
    try:
        # Fetch listings for the specified item and range
        listings, total_count, new_last_modified = await client.get_item_listings(
            item,
            App.DOTA2,  # Application ID for DOTA 2 market listings
            count=LISTINGS_PER_REQUEST,  # Maximum number of listings to fetch per request
            start=start,  # Starting offset for the listings
            if_modified_since=last_modified  # Use If-Modified-Since header if provided
        )

        # Log successful fetch
        print(f"[Worker {proxy}] Fetched listings for '{item}' starting at {start}, total listings available: {total_count}")

        # Parse the fetched listings into a DataFrame
        df = parse_market_listings(listings)
        if not df.empty:
            # Display the parsed listings
            print(f"[Worker {proxy}] Listings Data:\n{df.to_string(index=False)}")

            # Update the real-time storage with this data
            update_db_item_storage(item, df)
        else:
            # Log if no valid listings are found
            print(f"[Worker {proxy}] No valid listings to display.")

        # Return the fetched listings and the new last modified timestamp
        return listings, new_last_modified

    except Exception as e:
        # Handle and log errors during fetching
        print(f"[Worker {proxy}] Error fetching listings for '{item}' at start {start}: {e}")
        return None, None

    

# Worker function that fetches listing ranges for an item
async def worker(task_queue, proxy):
    """
    Worker function that processes tasks from the task_queue to fetch item listings.

    Parameters:
        task_queue (asyncio.Queue): Queue containing tasks to be processed.
        proxy (str): Proxy used for making requests.
    """
    client = SteamPublicClient(proxy=proxy)  # Create a Steam client instance with the provided proxy
    last_request_time = time.time()  # Track the time of the last request to enforce delays
    listings_fetched = 0  # Counter to track the number of listings fetched by this worker

    try:
        while True:
            # Get the next task from the task_queue
            task = await task_queue.get()
            
            # Check for a sentinel task (None) to signal worker exit
            if task is None:
                logging.debug(f"Worker with proxy {proxy} received sentinel. Exiting.")
                break

            # Unpack task details
            item, start, last_modified = task
            logging.debug(f"Worker with proxy {proxy} processing item: {item}, start: {start}")

            try:
                # Fetch listings for the specified item and range
                listings, new_last_modified = await fetch_listings_for_item_range(
                    client, item, start, proxy, last_modified
                )
                last_request_time = time.time()  # Update the time of the last request
                listings_fetched += LISTINGS_PER_REQUEST  # Increment the listings fetched counter

                # If listings are fetched successfully, parse and process them
                if listings is not None:
                    df = parse_market_listings(listings)  # Parse the listings into a DataFrame
                    if not df.empty:
                        logging.debug(f"Worker with proxy {proxy} parsed {len(df)} listings for {item}.")
                    else:
                        logging.debug(f"Worker with proxy {proxy} found no valid listings for {item}.")

            except Exception as e:
                # Log the error, add a delay, and re-add the task to the queue for retry
                logging.error(f"Worker with proxy {proxy} encountered error: {e}. Retrying.")
                await asyncio.sleep(5)  # Small delay before retrying
                await task_queue.put(task)  # Re-add the task to the queue for retry
                continue

            finally:
                # Mark the task as done in the queue
                logging.debug(f"Worker with proxy {proxy} marking task as done: {task}")
                task_queue.task_done()

            # Enforce a delay if the number of listings fetched exceeds the batch limit
            if listings_fetched >= LISTINGS_BEFORE_BATCH_DELAY:
                logging.debug(f"Worker with proxy {proxy} taking a batch delay.")
                await asyncio.sleep(BATCH_DELAY)
                listings_fetched = 0  # Reset the listings fetched counter

    finally:
        # Close the client session when the worker finishes
        await client.session.close()
        logging.debug(f"Worker with proxy {proxy} has exited and closed its session.")




# Worker function to fetch total listings and enqueue listing tasks
async def total_listings_worker(total_task_queue, task_queue, proxy):
    """
    Worker function that processes items from the total_task_queue to fetch 
    the total number of listings for each item. Tasks are then split into chunks
    and added to the main task_queue for further processing.

    Parameters:
        total_task_queue (asyncio.Queue): The queue containing items to fetch total listings for.
        task_queue (asyncio.Queue): The main queue where detailed listing tasks will be added.
        proxy (str): The proxy used for making requests.
    """
    client = SteamPublicClient(proxy=proxy)  # Create a Steam client with the specified proxy
    try:
        # Process items from the total_task_queue until it is empty
        while not total_task_queue.empty():
            # Fetch the next item from the total task queue
            item = await total_task_queue.get()
            print(f"[Total Listings Worker {proxy}] Fetching total listings for item: {item}")
            
            # Fetch the total number of listings for the item
            total_count = await fetch_total_listings(client, item)
            
            # Break the total count into chunks and create tasks for each chunk
            for start in range(0, total_count, LISTINGS_PER_REQUEST):
                task = (item, start, None)  # (item, start offset, last_modified)
                print(f"[Total Listings Worker {proxy}] Enqueuing task: {task}")
                await task_queue.put(task)  # Add the task to the main task queue
            
            # Mark the item task as done in the total task queue
            total_task_queue.task_done()
            
            # Apply a delay to avoid hitting rate limits
            await asyncio.sleep(REQUEST_DELAY)
            
    finally:
        # Close the client session when the worker finishes
        await client.session.close()
        print(f"[Total Listings Worker {proxy}] has closed its session.")



# Adjust gem buy orders after finding a profitable item
def adjust_gem_buy_orders(gem_name):
    """
    Adjust the buy order graph for a gem after a profitable item has been found.
    """
    #gem_data = real_time_gem_price_storage.get(gem_name)

    gem_data = fetch_gem_from_db(gem_name)

    if not gem_data or not gem_data['buy_orders']:
        logging.debug(f"No buy orders available for gem '{gem_name}' to adjust.")
        return

    # Decrement the quantity of the highest buy order
    if gem_data['buy_orders'][0][1] > 1:
        gem_data['buy_orders'][0][1] -= 1
    else:
        # If the top buy order quantity is 1, remove it from the list
        gem_data['buy_orders'].pop(0)

    logging.debug(f"Updated buy orders for gem '{gem_name}': {gem_data['buy_orders']}")
    #real_time_gem_price_storage[gem_name] = gem_data  # Save back the adjusted data

    update_gem_in_db(
        gem_name,
        gem_data
    )


def normalize_gem_name(gem_name, gem_type=None):
    """
    Normalize gem names to match the database structure. Adds prefixes like 'Ethereal:' or 'Prismatic:'.
    """
    if gem_type:
        return f"{gem_type}: {gem_name}".strip()
    return gem_name.strip()



async def send_profitable_item_alert(comparison_result, action):
    """
    Constructs a structured message for profitable items and triggers an asynchronous alert.

    Parameters:
        comparison_result (dict): The result of the comparison, including profitability status and item details.
        action (str): "inserted" or "updated" to indicate the database operation.
    """
    global profitable_item_found
    
    try:
        # Fetch the item's name from the database for inclusion in the alert
        item_details = fetch_item_from_db(comparison_result["item_id"])
        if not item_details:
            logging.error(f"Unable to fetch item details for ID: {comparison_result['item_id']}")
            return

        # Calculate profit
        profit = (
            comparison_result["combined_gem_price"] * (1 - STEAM_FEE) - comparison_result["item_price"]
            if "combined_gem_price" in comparison_result
            else comparison_result["highest_buy_order_price"] * (1 - STEAM_FEE) - comparison_result["item_price"]
        )

        # Construct the structured message
        message = (
            f"Profitable item found ({action.upper()})! "
            f"\nName: {item_details['name']}; "
            f"\nPrice: {comparison_result['item_price']}; "
            f"\nCombined gem price: {comparison_result.get('combined_gem_price', 'N/A')}; "
            f"\nProfit: {profit:.2f}; "
            f"\nID: {comparison_result['item_id']}"
        )
        
        logging.info(f"Sending alert: {message}")
        profitable_item_found = True
        
        await telegram_bot.event_trigger(message, "Prismatic_Parser_Bot")

    except Exception as e:
        logging.error(f"Error in send_profitable_item_alert: {e}")



async def synchronize_and_compare(item_id, fetch_start, fetch_end):
    """
    Synchronizes item data with corresponding gem data, performs comparisons, 
    and handles the results (e.g., stores in the database or triggers alerts).

    Parameters:
        item_id (str): The ID of the item to synchronize and compare.
    """
    try:
        if not fetch_start or not fetch_end:
            logging.error("Fetch timestamps are missing. Cannot proceed with synchronization.")
            return

        # Fetch the item data from the database
        item_data = fetch_item_from_db(item_id)
        logging.debug(f"Fetched item data: {item_data}")

        if not item_data:
            logging.debug(f"No data found for item_id: {item_id}")
            return  # Exit if no data is found for the item

        # Check if the item's timestamp is within the last fetch cycle
        item_timestamp = item_data.get("timestamp")
        if not item_timestamp or not (fetch_start <= item_timestamp <= fetch_end):
            logging.debug(f"Item {item_id} is outside the last fetch cycle. Skipping.")
            return  # Exit if the item's timestamp is not within the last fetch cycle

        # Extract and validate item description
        item_description = item_data.get("name")
        if not item_description:
            logging.debug(f"Missing item description for item_id: {item_id}")
            return  # Exit if the item has no description

        logging.debug(f"Processing item: {item_description}")

        # If the item is a courier, perform a combined comparison
        if item_description in COURIERS:
            # Normalize gem names
            prismatic_gem_name = normalize_gem_name(item_data.get('prismatic_gem', ''), gem_type="Prismatic")
            ethereal_gem_name = normalize_gem_name(item_data.get('ethereal_gem', ''), gem_type="Ethereal")
            logging.debug(f"Normalized gems - Prismatic: {prismatic_gem_name}, Ethereal: {ethereal_gem_name}")

            # Fetch gem data for both Prismatic and Ethereal gems
            prismatic_gem_data = fetch_gem_from_db(prismatic_gem_name)
            ethereal_gem_data = fetch_gem_from_db(ethereal_gem_name)
            logging.debug(f"Fetched gem data - Prismatic: {prismatic_gem_data}, Ethereal: {ethereal_gem_data}")

            if not prismatic_gem_data or not ethereal_gem_data:
                logging.debug(f"Incomplete gem data for item_id {item_id}")
                return  # Exit if data for either gem is missing

            # Perform the comparison with the combined gem price
            comparison_result = compare_item_with_combined_gem_price(item_data, prismatic_gem_data, ethereal_gem_data)

        # If the item is not a courier, perform a single-gem comparison
        else:
            # Normalize Prismatic gem name
            prismatic_gem_name = normalize_gem_name(item_data.get('prismatic_gem', ''), gem_type="Prismatic")
            # Fetch Prismatic gem data
            prismatic_gem_data = fetch_gem_from_db(prismatic_gem_name)
            logging.debug(f"Fetched Prismatic gem data: {prismatic_gem_data}")

            if not prismatic_gem_data:
                logging.debug(f"No gem data for Prismatic gem '{prismatic_gem_name}' for item_id '{item_id}'")
                return  # Exit if no data for the Prismatic gem is found

            # Perform the comparison with a single gem
            comparison_result = compare_item_with_gem(item_data, prismatic_gem_data)

        # Log the comparison result
        logging.debug(f"Comparison result: {comparison_result}")

        # If the item is profitable, write it to the database and send an alert
        if comparison_result["is_profitable"]:
            action = write_comparison_to_db(comparison_result)  # Get whether it was inserted or updated
            logging.info(f"Profitable item found and written to database: {comparison_result}")
            await send_profitable_item_alert(comparison_result, action)
        else:
            logging.debug(f"Item not profitable: {comparison_result}")
            action = remove_comparison_if_not_profitable(comparison_result)
            if action == "removed":
                logging.info(f"Non-profitable comparison for item_id {comparison_result['item_id']} was removed.")
            elif action == "not_found":
                logging.debug(f"No comparison found to remove for item_id {comparison_result['item_id']}.")

    except Exception as e:
        logging.error(f"Error in synchronize_and_compare: {e}")





# Compare item price with the highest available buy order for its gem
def compare_item_with_gem(item_data, gem_data):
    """
    Compare item price with the highest available buy order for its gem.
    Returns a result dictionary including whether the item is profitable.
    """
    try:
        logging.debug(f"Comparing item: {item_data}, with gem data: {gem_data}")
        item_price = item_data['price']
        buy_orders = gem_data.get('buy_orders', {}).get('buy_orders', [])
        if not buy_orders:
            logging.debug(f"No buy orders available for gem: {gem_data['name']}")
            return {"is_profitable": False}

        prismatic_highest_buy_order = buy_orders[0][0]  # Access first price
        logging.debug(f"Highest buy order price: {prismatic_highest_buy_order}")

        # Determine if the item price is below the buy order price minus fees
        is_profitable = item_price < prismatic_highest_buy_order * (1 - STEAM_FEE - TARGET_PROFIT)
        comparison_result = {
            "item_id": item_data['id'],
            "item_price": item_price,
            "prismatic_gem_price": prismatic_highest_buy_order,
            "ethereal_gem_price": 0,
            "combined_gem_price": prismatic_highest_buy_order,
            "is_profitable": is_profitable,
            "timestamp": time.time()
        }

        logging.debug(f"Comparison result: {comparison_result}")
        return comparison_result
    except Exception as e:
        logging.error(f"Error in compare_item_with_gem: {e}")
        return {"is_profitable": False}



def compare_item_with_combined_gem_price(item_data, prismatic_gem_data, ethereal_gem_data):
    """
    Compare item price with the combined highest available buy order for Prismatic and Ethereal gems.
    Returns a result dictionary including whether the item is profitable.
    """
    try:
        logging.debug(f"Comparing item: {item_data} with Prismatic gem: {prismatic_gem_data}, Ethereal gem: {ethereal_gem_data}")
        item_price = item_data['price']

        # Extract buy orders
        prismatic_buy_orders = prismatic_gem_data.get('buy_orders', {}).get('buy_orders', [])
        ethereal_buy_orders = ethereal_gem_data.get('buy_orders', {}).get('buy_orders', [])

        if not prismatic_buy_orders or not ethereal_buy_orders:
            logging.debug(f"Missing buy orders for Prismatic: {prismatic_gem_data['name']} or Ethereal: {ethereal_gem_data['name']}")
            return {"is_profitable": False}

        prismatic_highest_buy_order = prismatic_buy_orders[0][0]  # Get first price
        ethereal_highest_buy_order = ethereal_buy_orders[0][0]    # Get first price

        logging.debug(f"Prismatic highest buy order: {prismatic_highest_buy_order}, Ethereal highest buy order: {ethereal_highest_buy_order}")

        # Calculate combined gem price
        combined_gem_price = prismatic_highest_buy_order + ethereal_highest_buy_order

        # Determine if the item price is below the combined gem price minus fees
        is_profitable = item_price < combined_gem_price * (1 - STEAM_FEE - TARGET_PROFIT)
        comparison_result = {
            "item_id": item_data['id'],
            "item_price": item_price,
            "prismatic_gem_price": prismatic_highest_buy_order,
            "ethereal_gem_price": ethereal_highest_buy_order,
            "combined_gem_price": combined_gem_price,
            "is_profitable": is_profitable,
            "timestamp": time.time()
        }

        logging.debug(f"Combined comparison result: {comparison_result}")
        return comparison_result
    except Exception as e:
        logging.error(f"Error in compare_item_with_combined_gem_price: {e}")
        return {"is_profitable": False}


# Set up a logger for alerts
alert_logger = logging.getLogger("alert_logger")
alert_logger.setLevel(logging.INFO)

# Create a file handler for the alerts
alert_file_handler = logging.FileHandler("./logs/alerts_new.log")
alert_file_handler.setLevel(logging.INFO)

# Create a logging format
formatter = logging.Formatter('%(asctime)s - %(message)s')
alert_file_handler.setFormatter(formatter)

# Add the file handler to the alert logger
alert_logger.addHandler(alert_file_handler)

def alert_logs_and_cmd(comparison_result):
    """
    Trigger an alert if the comparison result shows a profitable opportunity and log it to a separate file.
    """
    if comparison_result["is_profitable"]:
        if "combined_gem_price" in comparison_result:
            # This is a courier item with both Prismatic and Ethereal gems
            alert_message = (
                f"Profitable opportunity found! Item ID: {comparison_result['item_id']}, "
                f"Item Price: {comparison_result['item_price']}, "
                f"Prismatic Gem Price: {comparison_result['prismatic_gem_price']}, "
                f"Ethereal Gem Price: {comparison_result['ethereal_gem_price']}, "
                f"Combined Gem Price: {comparison_result['combined_gem_price']}"
            )
            print(alert_message)
            alert_logger.info(alert_message)  # Log to separate file
        else:
            # This is a regular item with a single gem
            alert_message = (
                f"Profitable opportunity found! Item ID: {comparison_result['item_id']}, "
                f"Gem: {comparison_result['gem_name']}, Item Price: {comparison_result['item_price']}, "
                f"Buy Order Price: {comparison_result['highest_buy_order_price']}"
            )
            print(alert_message)
            alert_logger.info(alert_message)  # Log to separate file


def write_comparison_to_db(comparison_result):
    """
    Writes the comparison result to the comparisons database.
    Updates an existing entry if the item_id already exists, otherwise inserts a new entry.

    Parameters:
        comparison_result (dict): The comparison data.

    Returns:
        str: "inserted" if a new entry was added, "updated" if an existing entry was modified.
    """
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    # Calculate the expected profit
    combined_gem_price = comparison_result.get("combined_gem_price", 0)
    item_price = comparison_result.get("item_price", 0)
    expected_profit = combined_gem_price * (1 - STEAM_FEE) - item_price
    expected_profit = round(expected_profit, 2)

    # Check if an entry with the same item_id already exists
    cursor.execute("SELECT COUNT(*) FROM comparisons WHERE item_id = ?", (comparison_result.get("item_id"),))
    exists = cursor.fetchone()[0] > 0

    if exists:
        # Update the existing comparison
        cursor.execute("""
        UPDATE comparisons
        SET item_price = ?, is_profitable = ?, timestamp = ?, 
            prismatic_gem_price = ?, ethereal_gem_price = ?, 
            combined_gem_price = ?, expected_profit = ?
        WHERE item_id = ?
        """, (
            item_price,
            comparison_result.get("is_profitable"),
            comparison_result.get("timestamp"),
            comparison_result.get("prismatic_gem_price", 0),
            comparison_result.get("ethereal_gem_price", 0),
            combined_gem_price,
            expected_profit,
            comparison_result.get("item_id")
        ))
        action = "updated"
    else:
        # Insert a new comparison
        cursor.execute("""
        INSERT INTO comparisons (
            item_id, item_price, is_profitable, 
            timestamp, prismatic_gem_price, ethereal_gem_price, 
            combined_gem_price, expected_profit
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            comparison_result.get("item_id"),
            item_price,
            comparison_result.get("is_profitable"),
            comparison_result.get("timestamp"),
            comparison_result.get("prismatic_gem_price", 0),
            comparison_result.get("ethereal_gem_price", 0),
            combined_gem_price,
            expected_profit
        ))
        action = "inserted"

    conn.commit()
    conn.close()

    # Log the action performed
    logging.info(f"Comparison for item_id {comparison_result.get('item_id')} was {action}. Expected profit: {expected_profit:.2f}")
    return action  # Return whether it was inserted or updated


def remove_comparison_if_not_profitable(comparison_result):
    """
    Removes the comparison result from the comparisons database if the item is not profitable.

    Parameters:
        comparison_result (dict): The comparison data.

    Returns:
        str: "removed" if the entry was deleted, "not_found" if no entry was found.
    """
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()

    try:
        # Check if an entry with the same item_id already exists
        cursor.execute("SELECT COUNT(*) FROM comparisons WHERE item_id = ?", (comparison_result.get("item_id"),))
        exists = cursor.fetchone()[0] > 0

        if exists:
            if not comparison_result["is_profitable"]:
                # Delete the entry if the item is not profitable
                cursor.execute("DELETE FROM comparisons WHERE item_id = ?", (comparison_result.get("item_id"),))
                conn.commit()
                action = "removed"
                logging.info(f"Comparison for item_id {comparison_result.get('item_id')} was removed as it was not profitable.")
            else:
                action = "not_removed"
                logging.debug(f"Comparison for item_id {comparison_result.get('item_id')} was not removed as it is profitable.")
        else:
            action = "not_found"
            logging.debug(f"No comparison found for item_id {comparison_result.get('item_id')} to remove.")

    except Exception as e:
        logging.error(f"Error while attempting to remove comparison for item_id {comparison_result.get('item_id')}: {e}")
        action = "error"

    finally:
        conn.close()

    return action




# Main Monitor Function
async def monitor(main_finished):
    """
    Monitors items only when `main_finished` is set.
    Runs monitoring only once during the fetchers' cooldown period.
    """
    global profitable_item_found

    try:
        while True:
            # Wait until main fetcher has finished
            await main_finished.wait()
            logging.debug("Monitor: Main fetcher finished. Starting monitoring.")

            # Retrieve the latest fetch timestamps
            fetch_start, fetch_end = get_last_fetch_timestamps()
            logging.debug(f"Monitor: Retrieved timestamps - fetch_start={fetch_start}, fetch_end={fetch_end}")

            # Fetch all items from the database
            all_items = fetch_all_items_from_db()
            logging.debug(f"Monitor: Fetched {len(all_items)} items from the database.")

            # Reset profitable item flag
            profitable_item_found = False

            # Process all items and check profitability
            for item in all_items:
                logging.debug(f"Monitoring item: {item}")
                await synchronize_and_compare(item['id'], fetch_start, fetch_end)
                await asyncio.sleep(0.05)  # Prevent tight looping

            logging.debug("Monitor: Monitoring completed. Waiting for next fetcher cycle.")

            # Send alert if no profitable items were found
            if not profitable_item_found:
                await send_no_profitable_items_alert(fetch_start, fetch_end)

            # Wait for the next fetch cycle
            main_finished.clear()  # Clear the flag to wait for the next main cycle
            await main_finished.wait()  # Wait for the next main cycle to finish

            logging.debug("Monitor: Waiting for new fetcher cycles to complete before monitoring again.")

    except Exception as e:
        logging.error("Error in monitoring: %s", e)


async def unlock_proxies(proxy_ids):
    """Unlocks all proxies after use"""
    try:
        if proxy_ids:
            proxy_api.unlock_proxies(proxy_ids)
            logging.info(f"Unlocked {len(proxy_ids)} proxies")
    except Exception as e:
        logging.error(f"Error unlocking proxies: {e}")


async def get_formatted_proxies():
    """
    Fetches and formats available proxies, retrying until successful.
    
    Returns:
        tuple: (list of formatted proxy strings, list of proxy IDs)
    """
    while True:
        try:
            available_proxies = proxy_api.get_all_available_proxies()
            if not available_proxies:
                logging.error("No proxies available. Waiting before retry...")
                await asyncio.sleep(300)  # Wait 5 minutes before retrying
                continue

            # Format proxy strings
            format_proxy = lambda p: f"{p['protocol']}://{p.get('username', '')}:{p.get('password', '')}@{p['ip']}:{p['port']}"
            proxy_strings = [format_proxy(p) for p in available_proxies]
            proxy_ids = [p['id'] for p in available_proxies]

            logging.info(f"Successfully obtained {len(proxy_strings)} proxies")
            return proxy_strings, proxy_ids

        except Exception as e:
            logging.error(f"Error fetching proxies: {e}")
            await asyncio.sleep(300)  # Wait 5 minutes before retrying

def split_proxies(proxy_strings, proxy_ids):
    """
    Splits proxies between gem and main workers.
    
    Args:
        proxy_strings (list): List of formatted proxy strings
        proxy_ids (list): List of proxy IDs
    
    Returns:
        tuple: (gem_proxies, main_proxies, gem_proxy_ids, main_proxy_ids)
    """
    total_proxies = len(proxy_strings)
    gem_proxy_count = math.ceil(total_proxies * 0.2)  # 20% for gem fetcher
    
    # Split proxy strings
    gem_proxies = proxy_strings[:gem_proxy_count]
    main_proxies = proxy_strings[gem_proxy_count:]
    
    # Split proxy IDs
    gem_proxy_ids = proxy_ids[:gem_proxy_count]
    main_proxy_ids = proxy_ids[gem_proxy_count:]

    return gem_proxies, main_proxies, gem_proxy_ids, main_proxy_ids

async def main(main_started, main_finished, gem_proxy_queue):
    """Main function to periodically fetch listings for items and signal when finished."""
    global profitable_item_found

    while True:
        logging.info("Main: Starting new fetch cycle")
        main_started.set()  # Signal start of new cycle
        profitable_item_found = False
        main_cycle_start_timestamp = time.time()

        try:
            # Get proxies once for both main and gem fetcher
            logging.info("Main: Fetching proxies for both main and gem workers")
            proxy_strings, proxy_ids = await get_formatted_proxies()
            logging.info(f"Main: Successfully obtained {len(proxy_strings)} total proxies")

            # Split proxies
            gem_proxies, main_proxies, gem_proxy_ids, main_proxy_ids = split_proxies(proxy_strings, proxy_ids)
            logging.info(f"Main: Split proxies - Main: {len(main_proxies)}, Gem: {len(gem_proxies)}")
            
            # Create task queues
            total_task_queue = asyncio.Queue()
            task_queue = asyncio.Queue()
            
            # Share gem proxies through the passed queue
            logging.info(f"Main: Sharing {len(gem_proxies)} gem proxies through queue")
            for i in range(len(gem_proxies)):
                await gem_proxy_queue.put((gem_proxies[i], gem_proxy_ids[i]))
            logging.info("Main: Finished sharing gem proxies")

            # Populate queues
            item_count = len(ITEMS) + len(COURIERS)
            logging.info(f"Main: Populating task queue with {item_count} items")
            for item in ITEMS + COURIERS:
                await total_task_queue.put(item)
            logging.info("Main: Task queue populated")

            # Start total workers
            logging.info(f"Main: Starting {len(main_proxies)} total listing workers")
            total_workers = [total_listings_worker(total_task_queue, task_queue, proxy) 
                           for proxy in main_proxies]
            await asyncio.gather(*total_workers)
            logging.info("Main: Total workers completed")

            # Add sentinel tasks
            logging.info(f"Main: Adding {len(main_proxies)} sentinel tasks")
            for _ in range(len(main_proxies)):
                await task_queue.put(None)
            logging.info("Main: Sentinel tasks added")

            # Start main workers
            logging.info(f"Main: Starting {len(main_proxies)} main workers")
            main_workers = [worker(task_queue, proxy) for proxy in main_proxies]
            await asyncio.gather(*main_workers)
            logging.info("Main: Main workers completed")

            if total_task_queue.empty() and task_queue.empty():
                logging.info("Main: All tasks completed, setting finished flag")
                main_finished.set()
                main_cycle_end_timestamp = time.time()
                cycle_duration = main_cycle_end_timestamp - main_cycle_start_timestamp
                logging.info(f"Main: Cycle completed in {cycle_duration:.2f} seconds")
                save_fetch_timestamps(main_cycle_start_timestamp, main_cycle_end_timestamp)

        except Exception as e:
            logging.error(f"Main: Error in main cycle: {e}", exc_info=True)

        finally:
            logging.info(f"Main: Unlocking {len(main_proxy_ids)} main proxies")
            await unlock_proxies(main_proxy_ids)
            logging.info("Main: Proxies unlocked")
         
        # Wait before next cycle
        logging.info("Main: Starting cooldown period (900 seconds)")
        main_started.clear()  # Clear the flag before sleeping
        await asyncio.sleep(900)
        logging.info("Main: Resetting flags for next cycle")
        main_finished.clear()

async def main_gem_histogram_fetcher(main_started, gem_proxy_queue):
    """Periodically fetches gem data using dedicated proxies."""
    while True:
        logging.info("Gem Fetcher: Waiting for main to start")
        await main_started.wait()  # Wait for main to start
        main_started.clear()  # Clear the flag so we'll wait for the next cycle
        logging.info("Gem Fetcher: Starting new fetch cycle")
        
        try:
            # Get proxies from queue
            logging.info("Gem Fetcher: Retrieving proxies from queue")
            gem_proxies = []
            gem_proxy_ids = []
            
            # Collect proxies until sentinel is received
            while True:
                try:
                    logging.debug("Gem Fetcher: Waiting for next proxy from queue")
                    proxy_data = await asyncio.wait_for(gem_proxy_queue.get(), timeout=5.0)
                    logging.debug(f"Gem Fetcher: Received proxy data: {proxy_data}")
                    
                    if proxy_data is None:  # Check for sentinel
                        logging.debug("Gem Fetcher: Received sentinel, breaking proxy collection loop")
                        gem_proxy_queue.task_done()
                        break
                        
                    proxy, proxy_id = proxy_data
                    logging.debug(f"Gem Fetcher: Adding proxy {proxy_id} to collection")
                    gem_proxies.append(proxy)
                    gem_proxy_ids.append(proxy_id)
                    gem_proxy_queue.task_done()
                    
                except asyncio.TimeoutError:
                    logging.warning("Gem Fetcher: Timeout waiting for proxies, breaking collection loop")
                    break

            logging.info(f"Gem Fetcher: Retrieved {len(gem_proxies)} proxies from queue")

            if not gem_proxies:
                logging.warning("Gem Fetcher: No proxies available, waiting for next cycle")
                continue

            # Load gems data and create tasks
            logging.info("Gem Fetcher: Loading gems data")
            gems_data = load_gems_data()
            gem_task_queue = asyncio.Queue()

            # Create tasks
            valid_gems = 0
            for gem_name, gem_info in gems_data.items():
                if item_name_id := gem_info.get("id"):
                    await gem_task_queue.put((gem_name, item_name_id))
                    valid_gems += 1
            logging.info(f"Gem Fetcher: Created {valid_gems} tasks for processing")

            # Process gems with available proxies
            logging.info(f"Gem Fetcher: Starting {len(gem_proxies)} worker tasks")
            gem_tasks = [fetch_gem_data_worker(gem_task_queue, proxy) 
                       for proxy in gem_proxies]
            await asyncio.gather(*gem_tasks)
            logging.info("Gem Fetcher: All gem tasks completed")

        except Exception as e:
            logging.error(f"Gem Fetcher: Error in fetch cycle: {e}", exc_info=True)
        
        finally:
            if gem_proxy_ids:
                logging.info(f"Gem Fetcher: Unlocking {len(gem_proxy_ids)} gem proxies")
                await unlock_proxies(gem_proxy_ids)
                logging.info("Gem Fetcher: Proxies unlocked")
            
            logging.info("Gem Fetcher: Waiting for next cycle")
            # Don't start a new cycle here, just continue to the top of the loop
            # where it will wait for main_started

async def main_all():
    """
    Coordinates `main`, `main_gem_histogram_fetcher`, and `monitor`.
    """
    main_started = asyncio.Event()  # Flag for `main` start
    main_finished = asyncio.Event()  # Flag for `main` completion
    gem_proxy_queue = asyncio.Queue()  # Queue for sharing gem proxies
            
    logging.debug("Starting all tasks.")
    await asyncio.gather(
        main(main_started, main_finished, gem_proxy_queue),
        main_gem_histogram_fetcher(main_started, gem_proxy_queue),
        monitor(main_finished),
        telegram_bot.background_bot_polling()  # Start bot polling as a background task
    )
    logging.debug("All tasks stopped.")



if __name__ == "__main__":
    try:
        # Load environment variables
        load_dotenv()

        # Global rate limit configuration
        REQUEST_DELAY = int(os.getenv("REQUEST_DELAY"))  # Delay between individual requests for each worker
        BATCH_DELAY = int(os.getenv("BATCH_DELAY"))    # Delay after every 100 listings
        LISTINGS_PER_REQUEST = int(os.getenv("LISTINGS_PER_REQUEST"))  # Number of listings per request
        LISTINGS_BEFORE_BATCH_DELAY = int(os.getenv("LISTINGS_BEFORE_BATCH_DELAY"))  # Trigger batch delay after this count
        if not REQUEST_DELAY or not BATCH_DELAY or not LISTINGS_PER_REQUEST or not LISTINGS_BEFORE_BATCH_DELAY:
            raise ValueError("Global rate limit configuration not found in environment variables")
        
        ITEMS, COURIERS, ALLOWED_GEMS_PRISMATIC, ALLOWED_GEMS_ETHEREAL = load_constants()
        if not ITEMS or not COURIERS or not ALLOWED_GEMS_PRISMATIC or not ALLOWED_GEMS_ETHEREAL:
            raise ValueError("Items, couriers, prismatic gems, or ethereal gems data not found in environment variables")
        
        TARGET_PROFIT = float(os.getenv("TARGET_PROFIT"))
        STEAM_FEE = float(os.getenv("STEAM_FEE"))   
        if not TARGET_PROFIT or not STEAM_FEE:
            raise ValueError("Target profit or Steam fee not found in environment variables")
        
        BOT_TOKEN = os.getenv('BOT_TOKEN')
        CHAT_ID = os.getenv('CHAT_ID')
        api_key = os.getenv("API_KEY")

        
        if not BOT_TOKEN or not CHAT_ID or not api_key:
            raise ValueError("Bot token or Chat ID or API key not found in environment variables")
        
        proxy_api = ProxyAPI(
            api_key
        )
        # Initialize the bot globally
        telegram_bot = TelegramAlertBot(
            token=BOT_TOKEN,
            user_id=CHAT_ID,
            merge_pattern="No profitable items found between"
        )

        # Initialize the database
        init_db()

        # Print confirmation and run the main async program
        print("Database initialized successfully.")

        # Run the main application
        asyncio.run(main_all())
    except Exception as e:
        print(f"Error during execution: {e}")
        logging.error(f"Error during execution: {e}")