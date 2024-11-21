# Steam Prismatic Gems Parser

![Project Banner](./images/github-header-image.png)  
![Python](https://img.shields.io/badge/python-3.8%2B-blue) 
![License](https://img.shields.io/badge/license-MIT-green) 
![Status](https://img.shields.io/badge/status-Demonstration-orange)  
![Version](https://img.shields.io/badge/version-1.0-blue)
![Platform](https://img.shields.io/badge/platform-Windows%20|%20MacOS-lightgrey)

## Introduction
**Steam Prismatic Gems Parser** is a demonstration project that monitors Steam marketplace items with gems, specifically focusing on Dota 2 couriers and Arcana Items. It uses asynchronous workers running in parallel to fetch market data and analyzes the profitability of items by comparing their prices with the combined value of their embedded gems.

---

## Table of Contents
- [Introduction](#introduction)
- [How It Works](#how-it-works)
- [Core Components](#core-components)
- [Data Flow](#data-flow)
- [Database Schema](#database-schema)
- [Profitability Analysis](#profitability-analysis)
- [Configuration](#configuration)
- [Technology Stack](#technology-stack)
- [License](#license)

---
## How it works

### 1. Couriers and Arcana Items on Dota 2 Steam Marketplace
- **Unusual Couriers**: All Unusual quality couriers have 2 embedded gems: Prismatic and Ethereal, and after being purchased, allow to extract the gems while destroying the Item itself. The gems can be re-sold on the Steam Marketplace.
  <img width="355" alt="image" src="https://github.com/user-attachments/assets/578e0ed4-9602-4616-9f15-e62eb92f17dd">
- **Arcana Items**: Some Arcana quality items have a Prismatic gem, and after being purchased, allow to extract the gem while destroying the Item itself. The gem can be re-sold on the Steam Marketplace.
  <img width="350" alt="image" src="https://github.com/user-attachments/assets/bad153a6-9978-417e-918f-01b4304a127b">
- **Profitability**: Some items are cheaper to buy than the price of re-selling the gems on the Steam Marketplace. Those items are considered profitable. (refer to #profitability-analysis)



## Core Components

### 1. Market Data Fetchers
- **Total Listings Worker**: Fetches the total number of available listings for each item
- **Gem Data Worker**: Retrieves and processes buy orders for both prismatic and ethereal gems
- **Rate Limiting**: Implements delays between requests (REQUEST_DELAY = 10s, BATCH_DELAY = 60s)

### 2. Data Processing
- **Market Listing Parser**: Extracts gem information using BeautifulSoup
- **Buy Order Processing**: Normalizes and processes gem buy order data
- **Database Operations**: Handles CRUD operations for items, gems, and comparisons

### 3. Monitoring System
- **Profitability Monitor**: Continuously checks for profitable opportunities
- **Alert System**: Sends notifications via Telegram when profitable items are found
- **Timestamp Tracking**: Maintains fetch cycles for historical analysis

---

## Data Flow

1. **Initialization**
   - Load environment variables (BOT_TOKEN, CHAT_ID)
   - Initialize Telegram bot
   - Set up database tables
   - Configure logging

2. **Main Cycle**
   ```
   Start Main Cycle
   ├── Fetch Total Listings
   │   └── Queue tasks for detailed fetching
   ├── Process Market Listings
   │   ├── Extract gem information
   │   └── Store in database
   └── Monitor Profitability
       ├── Compare prices
       └── Send alerts if profitable
   ```

3. **Gem Processing**
   - Fetch gem buy orders
   - Process histogram data
   - Update database with latest prices

4. **Monitoring**
   - Compare item prices with gem values
   - Calculate potential profit
   - Trigger alerts for profitable items

---

## Database Schema

The database contains three main tables:

### Items Table
| Column           | Type   | Description                                     |
|-------------------|--------|-------------------------------------------------|
| `id`             | TEXT   | Unique identifier for the item (Primary Key).   |
| `name`           | TEXT   | Name of the item.                               |
| `price`          | REAL   | Price of the item.                              |
| `ethereal_gem`   | TEXT   | Associated ethereal gem.                        |
| `prismatic_gem`  | TEXT   | Associated prismatic gem.                       |
| `timestamp`      | REAL   | Last updated timestamp.                         |

### Gems Table
| Column             | Type    | Description                                   |
|---------------------|---------|-----------------------------------------------|
| `name`             | TEXT    | Name of the gem (Primary Key).                |
| `buy_orders`       | TEXT    | JSON string containing the buy orders.        |
| `buy_order_length` | INTEGER | Number of buy orders.                         |
| `timestamp`        | REAL    | Last updated timestamp.                       |

### Comparisons Table
| Column                | Type    | Description                                   |
|------------------------|---------|-----------------------------------------------|
| `item_id`            | TEXT    | Unique identifier for the item (Primary Key). |
| `item_price`         | REAL    | Price of the item.                            |
| `is_profitable`      | BOOLEAN | Whether the item is profitable.               |
| `timestamp`          | REAL    | Last updated timestamp.                       |
| `prismatic_gem_price`| REAL    | Price of the associated prismatic gem.        |
| `ethereal_gem_price` | REAL    | Price of the associated ethereal gem.         |
| `combined_gem_price` | REAL    | Combined price of both gems.                  |
| `expected_profit`    | REAL    | Calculated expected profit for the item.      |

---

## Profitability Analysis

### How It Works

To determine profitability, the system compares the price of an item with the combined prices of the associated prismatic and ethereal gems:

1. **Extract Buy Orders**:
   - For both prismatic and ethereal gems, the highest buy order is retrieved from their respective buy orders list.

   - Let:
     - `P_prismatic` = highest prismatic gem buy order.
     - `P_ethereal` = highest ethereal gem buy order.

2. **Calculate Combined Gem Price**:
   - The combined gem price is calculated as:
     ```
     P_combined = P_prismatic + P_ethereal
     ```

3. **Profitability Check**:
   - An item is considered profitable if:
     ```
     P_item < P_combined * (1 - SteamFee - TargetProfit)
     ```
     where:
     - `P_item` is the item price.
     - `SteamFee` is the Steam transaction fee percentage.
     - `TargetProfit` is the desired profit margin.

4. **Result**:
   - The system outputs a dictionary containing:
     - `item_id`: The unique identifier of the item.
     - `item_price`: The current item price.
     - `prismatic_gem_price` and `ethereal_gem_price`.
     - `combined_gem_price`.
     - `is_profitable`: A boolean indicating profitability.
     - `timestamp`: The time of calculation.
   - If the item is profitable, the system **sends an alert** containing the item details, combined gem prices, and calculated profit values for immediate action.

---

## Configuration

### Proxy Configuration
Proxies are loaded from the `proxies/` directory. Ensure the following files are populated with valid proxy addresses:
- `proxies_items.txt`: Proxy list for item data fetching.
- `proxies_gems.txt`: Proxy list for gem data fetching.

### Constants
Key constants can be configured in the project settings:
- `LISTINGS_PER_REQUEST`: Number of listings fetched per request.
- `REQUEST_DELAY`: Delay between requests to avoid rate limits.
- `BATCH_DELAY`: Additional delay between processing batches of requests.

---

## Technology Stack

| Technology | Description | Version | Links |
|------------|-------------|---------|-------|
| Python | Core programming language | 3.8+ | [Python.org](https://www.python.org/) |
| SQLite | Lightweight database | 3.x | [SQLite.org](https://sqlite.org/index.html) |
| Pandas | Data manipulation library | 2.0+ | [Pandas Docs](https://pandas.pydata.org/) |
| BeautifulSoup4 | HTML parsing library | 4.x | [BS4 Docs](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) |
| AioSteamPy | Async Steam API client | Latest | [AioSteamPy](https://pypi.org/project/aiosteampy/) |
| python-dotenv | Environment variable management | Latest | [python-dotenv](https://pypi.org/project/python-dotenv/) |
| aiohttp | Async HTTP client/server | Latest | [aiohttp](https://docs.aiohttp.org/) |
| telegram-alert-bot | Async Telegram alert system | 0.1.0 | [telegram-alert-bot](https://github.com/pudjojotaro/telegram-alert-bot) |
| asyncio | Async I/O framework | Built-in | [asyncio Docs](https://docs.python.org/3/library/asyncio.html) |
| logging | Logging facility | Built-in | [logging Docs](https://docs.python.org/3/library/logging.html) |

---

## License
This project is licensed under:
```
MIT License
```
