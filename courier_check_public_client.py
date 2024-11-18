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



current_dir = os.path.dirname(os.path.abspath(__file__))
projects_dir = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.append(projects_dir)

from telegram_alert_bot import event_trigger, background_bot_polling  # type: ignore



# Global rate limit configuration
REQUEST_DELAY = 10  # Delay between individual requests for each worker
BATCH_DELAY = 60    # Delay after every 100 listings
LISTINGS_PER_REQUEST = 12  # Number of listings per request
LISTINGS_BEFORE_BATCH_DELAY = 100  # Trigger batch delay after this count

# List of items to fetch data for
ITEMS = [
    "Fractal Horns of Inner Abysm", 
    "Corrupted Fractal Horns of Inner Abysm"
    "Exalted Fractal Horns of Inner Abysm", "Inscribed Fractal Horns of Inner Abysm",
    "Autographed Fractal Horns of Inner Abysm",
    "Swine of the Sunken Galley", "Inscribed Swine of the Sunken Galley",
    "Exalted Swine of the Sunken Galley", "Autographed Swine of the Sunken Galley",
    "Corrupted Swine of the Sunken Galley"
]

COURIERS = [
    "Unusual Mango the Newt", "Unusual Blazing Hatchling",
    "Unusual Ageless Apothecary", "Unusual Hermid", "Unusual Flightless Dod",
    "Unusual Antipode Couriers", "Unusual Beaver Knight", "Unusual Virtus Werebear",
    "Unusual Blotto and Stick", "Unusual Grimoire The Book Wyrm", "Unusual Hexgill the Lane Shark",
    "Unusual Mok", "Unusual Waldi the Faithful", "Unusual Alphid of Lecaciida",
    "Unusual Deathripper", "Unusual Itsy", "Unusual Coco the Courageous",
    "Unusual Nimble Ben", "Unusual Yonex's Rage", "Unusual Morok's Mechanical Mediary",
    "Unusual Speed Demon", "Unusual Tickled Tegu", "Unusual Osky the Ottragon",
    "Unusual Stumpy - Nature's Attendant", "Unusual Trapjaw the Boxhound",
    "Unusual Garran Drywiz and Garactacus", "Unusual Skip the Delivery Frog",
    "Unusual Kupu the Metamorpher", "Unusual Fezzle-Feez the Magic Carpet Smeevil",
    "Unusual Mighty Boar", "Unusual Prismatic Drake", "Unusual Trusty Mountain Yak",
    "Unusual Butch", "Unusual Grimsneer", "Unusual Fearless Badger",
    "Unusual Enduring War Dog", "Unusual The Llama Llama", "Unusual Na'Vi's Weaselcrow",
    "Unusual Masked Fey, Lord of Tempests", "Unusual Babka the Bewitcher", "Unusual Lil' Nova",
    "Unusual Tinkbot", "Unusual Tory the Sky Guardian", "Unusual Arnabus the Fairy Rabbit",
    "Unusual Snowl", "Unusual Cluckles the Brave", "Unusual Azuremir",
    "Unusual Shagbark", "Unusual Captain Bamboo", "Unusual Porcine Princess Penelope",
    "Unusual Snelfret the Snail", "Unusual Throe", "Unusual Jin and Yin Fox Spirits",
    "Unusual Mechjaw the Boxhound", "Unusual Frull", "Unusual Oculopus",
    "Unusual Drodo the Druffin", "Unusual Lockjaw the Boxhound", "Unusual Murrissey the Smeevil",
    "Unusual Hollow Jack", "Unusual Baekho", "Unusual Coral the Furryfish",
    "Unusual Baby Roshan", "Unusual Moil the Fettered", "Unusual Jumo",
    "Unusual Bionic Birdie"
]


ALLOWED_GEMS = [
    "Unhallowed Ground", "Dredge Earth", "Brusque Britches Beige", "Tnim S'nnam", 
    "Reflection's Shade", "Explosive Burst", "Miasmatic Grey", "Ships in the Night", 
    "Red", "Dungeon Doom", "Plague Grey", "Crystalline Blue", "Orange", "Light Green", 
    "Earth Green", "Diretide Orange", "Deep Green", "Champion's Green", "Deep Blue", 
    "Verdant Green", "Vermillion Renewal", "Placid Blue", "Champion's Blue", "Gold", 
    "Champion's Purple", "Sea Green", "Purple", "Rubiline", "Blue", "Bright Purple", 
    "Summer Warmth", "Bright Green", "Blossom Red", "Ember Flame", "Cursed Black", 
    "Plushy Shag", "Creator's Light", "Pyroclastic Flow", "Midas Gold"
]


ALLOWED_GEMS_ETHEREAL = [
    "Affliction of Vermin",
    "Bleak Hallucination",
    "Burning Animus",
    "Butterfly Romp",
    "Champion's Aura 2012",
    "Champion's Aura 2013",
    "Champion's Aura 2014",
    "Crystal Rift",
    "Cursed Essence",
    "Diretide Blight",
    "Diretide Corruption",
    "Divine Essence",
    "Emerald Ectoplasm",
    "Ethereal Flame",
    "Felicity's Blessing",
    "Frostivus Frost",
    "Ionic Vapor",
    "Luminous Gaze",
    "New Bloom Celebration",
    "Orbital Decay",
    "Piercing Beams",
    "Resonant Energy",
    "Rubiline Sheen",
    "Searing Essence",
    "Self-Made Aura",
    "Spirit of Earth",
    "Spirit of Ember",
    "Sunfire",
    "Touch of Flame",
    "Touch of Frost",
    "Touch of Midas",
    "Trail of Burning Doom",
    "Trail of the Amanita",
    "Trail of the Lotus Blossom",
    "Triumph of Champions"
]

DATABASE_PATH = "data/items_data.db"

target_profit = 0.01
steam_fee = 0.132

# Configure logging to write to logs.txt file
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
            - fetch_end_timestamp: The timestamp when the fetch ended.

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
            print(f"[Worker {proxy}] Fetching data for gem: {gem_name}")

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
                    print(f"[Worker {proxy}] Buy order data fetched and stored for gem: {gem_name}")

            except Exception as e:
                # Log any errors encountered during processing
                print(f"[Worker {proxy}] Error fetching data for gem '{gem_name}': {e}")

            # Mark the task as done in the queue
            gem_task_queue.task_done()

            # Increment items processed counter
            items_processed += 1

            # Enforce a delay after processing two items to avoid rate-limiting
            if items_processed >= 3:
                print(f"[Worker {proxy}] Pausing for {REQUEST_DELAY} seconds after processing 3 items.")
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
                        matching_prismatic_gem = next((allowed for allowed in ALLOWED_GEMS if allowed in gem_text), None)
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
                        matching_gem = next((allowed for allowed in ALLOWED_GEMS if allowed in gem_text), None)
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



def send_profitable_item_alert(comparison_result, action):
    """
    Constructs a structured message for profitable items and triggers an asynchronous alert.

    Parameters:
        comparison_result (dict): The result of the comparison, including profitability status and item details.
        action (str): "inserted" or "updated" to indicate the database operation.
    """
    try:
        # Fetch the item's name from the database for inclusion in the alert
        item_details = fetch_item_from_db(comparison_result["item_id"])
        if not item_details:
            logging.error(f"Unable to fetch item details for ID: {comparison_result['item_id']}")
            return

        # Calculate profit
        profit = (
            comparison_result["combined_gem_price"] * (1 - steam_fee) - comparison_result["item_price"]
            if "combined_gem_price" in comparison_result
            else comparison_result["highest_buy_order_price"] * (1 - steam_fee) - comparison_result["item_price"]
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

        # Trigger the asynchronous event
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.ensure_future(event_trigger(message, "Prismatic Parser Bot"))  # Schedule the coroutine
        else:
            loop.run_until_complete(event_trigger(message, "Prismatic Parser Bot"))  # Run the coroutine to completion

    except Exception as e:
        logging.error(f"Error in send_profitable_item_alert: {e}")



def synchronize_and_compare(item_id):
    """
    Synchronizes item data with corresponding gem data, performs comparisons, 
    and handles the results (e.g., stores in the database or triggers alerts).

    Parameters:
        item_id (str): The ID of the item to synchronize and compare.
    """
    # Fetch the last fetch timestamps
    fetch_start, fetch_end = get_last_fetch_timestamps()

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
        send_profitable_item_alert(comparison_result, action)
    else:
        logging.debug(f"Item not profitable: {comparison_result}")
        action = remove_comparison_if_not_profitable(comparison_result)
        if action == "removed":
            logging.info(f"Non-profitable comparison for item_id {comparison_result['item_id']} was removed.")
        elif action == "not_found":
            logging.debug(f"No comparison found to remove for item_id {comparison_result['item_id']}.")





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
        is_profitable = item_price < prismatic_highest_buy_order * (1 - steam_fee - target_profit)
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
        is_profitable = item_price < combined_gem_price * (1 - steam_fee - target_profit)
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
    expected_profit = combined_gem_price * (1 - steam_fee) - item_price
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

async def monitor(main_finished, gem_fetcher_finished):
    """
    Monitors items and gems only when both `main_finished` and `gem_fetcher_finished` are set.
    Runs monitoring only once during the fetchers' cooldown period.
    """
    try:
        while True:
            # Wait until both fetcher functions have finished
            await asyncio.wait([
                asyncio.create_task(main_finished.wait()),
                asyncio.create_task(gem_fetcher_finished.wait())
            ])
            logging.debug("Monitor: Both fetchers finished. Starting monitoring.")

            # Fetch all items from the database
            all_items = fetch_all_items_from_db()
            logging.debug(f"Monitor: Fetched {len(all_items)} items from the database.")

            for item in all_items:
                logging.debug(f"Monitoring item: {item}")
                synchronize_and_compare(item['id'])
                await asyncio.sleep(0.1)  # Prevent tight looping

            logging.debug("Monitor: Monitoring completed. Waiting for next fetcher cycle.")

            # Wait for either `main_finished` or `gem_fetcher_finished` to clear
            # This ensures monitoring doesn't run again until a new fetch cycle begins
            await asyncio.wait([
                asyncio.create_task(main_finished.wait()),
                asyncio.create_task(gem_fetcher_finished.wait())
            ])
            while main_finished.is_set() and gem_fetcher_finished.is_set():
                await asyncio.sleep(1)

            logging.debug("Monitor: Waiting for new fetcher cycles to complete before monitoring again.")

    except Exception as e:
        logging.error("Error in monitoring: %s", e)


async def main(main_finished):
    """
    Main function to periodically fetch listings for items and signal when finished.

    Parameters:
        main_finished (asyncio.Event): Event to signal the completion of the fetch cycle.
    """
    try:
        while True:
            logging.debug("Main: Starting new fetch cycle.")
            main_cycle_start_timestamp = time.time()
            # Load proxies for handling requests
            proxies = load_proxies(proxy_type="items")
            # Create task queues
            total_task_queue = asyncio.Queue()  # Queue for initial tasks (items and couriers)
            task_queue = asyncio.Queue()       # Queue for processing listings fetched from total_task_queue

            # Populate the total task queue with items and couriers
            for item in ITEMS:
                await total_task_queue.put(item)
            for item in COURIERS:
                await total_task_queue.put(item)
            logging.debug(f"Main: Total tasks in total_task_queue: {total_task_queue.qsize()}")

            # Start workers to process tasks from the total_task_queue and populate the task_queue
            total_workers = [total_listings_worker(total_task_queue, task_queue, proxy) for proxy in proxies]
            await asyncio.gather(*total_workers)  # Wait for all total workers to complete
            logging.debug("Main: Total listings workers completed.")

            # Add sentinel tasks to task_queue to signal workers to exit
            # Each worker will exit upon encountering a `None` task
            for _ in range(len(proxies)):
                await task_queue.put(None)

            # Start workers to process tasks from the task_queue
            main_workers = [worker(task_queue, proxy) for proxy in proxies]
            await asyncio.gather(*main_workers)  # Wait for all main workers to complete
            logging.debug("Main: Main workers completed.")

            # Ensure both task queues are empty before signaling completion
            if total_task_queue.empty() and task_queue.empty():
                logging.debug("Main: All tasks completed. Setting main_finished.")
                # Signal the completion of the fetch cycle
                main_finished.set()

                main_cycle_end_timestamp = time.time()
                save_fetch_timestamps(main_cycle_start_timestamp, main_cycle_end_timestamp)
                # Wait for 60 minutes (cooldown period) before starting a new fetch cycle
                await asyncio.sleep(3600)
                # Clear the event flag for the next cycle
                
                main_finished.clear()

    except Exception as e:
        logging.error("Error in main: %s", e)






async def main_gem_histogram_fetcher(gem_fetcher_finished):
    """
    Periodically fetches gem data and signals when finished.
    """
    try:
        while True:
            logging.debug("Gem Fetcher: Starting new fetch cycle.")
            proxies = load_proxies(proxy_type="gems")
            gems_data = load_gems_data()
            gem_task_queue = asyncio.Queue()

            # Populate the gem task queue
            for gem_name, gem_info in gems_data.items():
                item_name_id = gem_info.get("id")
                if item_name_id:
                    await gem_task_queue.put((gem_name, item_name_id))

            logging.debug(f"Gem Fetcher: Total tasks in queue: {gem_task_queue.qsize()}")

            # Start workers to process the gem tasks
            gem_tasks = [fetch_gem_data_worker(gem_task_queue, proxy) for proxy in proxies]
            await asyncio.gather(*gem_tasks)
            logging.debug("Gem Fetcher: Workers completed.")

            # Signal completion
            gem_fetcher_finished.set()
            logging.debug("Gem Fetcher: Fetching completed. Waiting 60 minutes.")

            await asyncio.sleep(3600)  # Wait for the cooldown
            gem_fetcher_finished.clear()  # Reset the flag for the next cycle
    except Exception as e:
        logging.error("Error in gem fetcher: %s", e)



async def main_all():
    """
    Coordinates `main`, `main_gem_histogram_fetcher`, and `monitor`.
    """
    main_finished = asyncio.Event()  # Flag for `main` completion
    gem_fetcher_finished = asyncio.Event()  # Flag for `gem fetcher` completion

    logging.debug("Starting all tasks.")
    await asyncio.gather(
        main(main_finished),
        main_gem_histogram_fetcher(gem_fetcher_finished),
        monitor(main_finished, gem_fetcher_finished),
        background_bot_polling()  # Telegram bot polling runs concurrently
    )
    logging.debug("All tasks stopped.")



if __name__ == "__main__":
    try:
        # Initialize the database
        init_db()
        """
        all_items = fetch_all_items_from_db()
        for item in all_items:
            logging.debug(f"Item: {item}")
        """

        # Print confirmation and run the main async program
        print("Database initialized successfully.")
        asyncio.run(main_all())
    except Exception as e:
        print(f"Error during execution: {e}")
        logging.error(f"Error during execution: {e}")