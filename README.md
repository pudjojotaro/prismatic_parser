# Dota 2 Prismatic Gems Parser

![Project Banner](./images/github-header-image.png)  
![Python](https://img.shields.io/badge/python-3.8%2B-blue) 
![License](https://img.shields.io/badge/license-MIT-green) 
![Status](https://img.shields.io/badge/status-Demonstration-orange)  
![Version](https://img.shields.io/badge/version-1.0-blue)
![Platform](https://img.shields.io/badge/platform-Windows%20|%20MacOS-lightgrey)

## Introduction
**Dota 2 Prismatic Gems Parser** is a demonstration project that monitors Steam marketplace items with gems, specifically focusing on Dota 2 couriers and Arcana Items. It uses asynchronous workers running in parallel to fetch market data and analyzes the potential profitability of items by comparing their prices with the combined value of their embedded gems.

---

## Table of Contents
- [Introduction](#introduction)
- [How It Works](#how-it-works)
- [Core Components](#core-components)
- [Data Flow](#data-flow)
- [Database Schema](#database-schema)
- [Proxy Management](#proxy-management)
- [Profitability Analysis](#profitability-analysis)
- [Configuration](#configuration)
- [Technology Stack](#technology-stack)
- [License](#license)

---
## How it works
### Main Loop
1. **Proxy Acquisition and Distribution**  
   - The system retrieves available proxies using a FastAPI-based proxy API.
   - The proxies are then randomized and partitioned into two exclusive sets: one for gem data fetching and processing, and another for fetching item market listings.

2. **Concurrent Data Fetching**  
   - **Item Service:** Uses a shared queue to first retrieve total listing counts for items (including couriers), and then fetches detailed market listings. It parses each listing to extract gem information using a dedicated parser.
   - **Gem Service:** Uses a separate worker pool to retrieve gem buy order histograms from the proxies. It processes these histograms (with retry mechanisms and exponential backoff) and compares new buy order data with existing records in the database. If discrepancies exceed a threshold, the update is flagged accordingly.
   - Both services include robust error handling. Upon consecutive failures (after three retry attempts), tasks are requeued so that other available proxies can process them.

3. **Monitoring and Notification**  
   - After fetching and processing, the Monitoring Service compares item prices against the combined value of the associated gems.
   - If an item's profit potential exceeds the target threshold, the system sends an alert via Telegram.
   - All processes are timestamped to enable historical analysis of fetch cycles and profitability trends.

4. **Cleanup**  
   - Regardless of the outcome, proxies are unlocked at the end of each cycle to free them up for use by other projects.
   
### Couriers and Arcana Items on Dota 2 Steam Marketplace
- **Unusual Couriers**: Almost all Unusual quality couriers have 2 embedded gems: Prismatic and Ethereal, and after being purchased, allow to extract the gems while destroying the Item itself. The gems can be re-sold on the Steam Marketplace.

- **Arcana Items**: Some Arcana quality items have a Prismatic gem, and after being purchased, allow to extract the gem while destroying the Item itself. The gem can be re-sold on the Steam Marketplace.

<table>
  <tr>
    <td align="center">
      <img width="355" alt="Unusual Courier" src="https://github.com/user-attachments/assets/578e0ed4-9602-4616-9f15-e62eb92f17dd">
      <br>
      <strong>Unusual Courier</strong><br>
      <em>Champion's Green Prismatic Gem</em><br>
      <em>Champion's Aura 2013 Ethereal Gem</em><br>
    </td>
    <td align="center">
      <img width="355" alt="Arcana" src="https://github.com/user-attachments/assets/bad153a6-9978-417e-918f-01b4304a127b">
      <br>
      <strong>Arcana Item</strong><br>
      <em>Purple Prismatic Gem</em><br>
    </td>
  </tr>
</table>
  
- **Profitability**: Some items are cheaper to buy than the price of re-selling the gems on the Steam Marketplace. Those items are considered profitable. (refer to [Profitability Analysis](#profitability-analysis))



## Core Components

### 1. Market Data Fetchers
- **Item Service**: Retrieves total listings for each item, divides the tasks into batches, and processes market listings to extract gem data. Features include:
  - Asynchronous workers with independent queues.
  - Retry mechanisms with exponential backoff.
  - Requeuing failed tasks for robust error recovery.

### 2. Data Processing
- **Parsing Module**: Uses BeautifulSoup to extract gem names and other necessary details from the marketplace HTML.

### 3. Gem Data Workers
- **Gem Service**: Fetches gem histograms via proxies, processes buy orders, compares against existing buy orders using percentage difference checks, and updates the database accordingly. Implements a retry loop for connection-level errors, requeuing tasks to be processed by another worker if a proxy fails three times.

### 4. Proxy Service
- Contacts the FastAPI-Proxy-API to fetch all available proxies.
- Randomizes and partitions the proxy list into two non-overlapping sets (using a configurable ratio) so that gem and item workers do not share the same proxies.
- Unlocks proxies after each cycle to ensure continuous availability.

### 5. Monitoring and Alerting
- **Monitoring Service**: Compares each item's price with the computed combined gem price. Calculates the expected profit after accounting for Steam fees and target margins.
- **Telegram Alert Bot**: Sends immediate alerts if any item is deemed profitable.

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
| `expected_profit`    | REAL    | Calculated expected profit.                   |

---

## Proxy Management

- The **Proxy Service** obtains live proxies from a FastAPI-driven Proxy API and formats them as connection URLs.
- Before each cycle, the proxies are randomized and then partitioned between gem workers and item workers using a configurable ratio. This ensures that the same proxy is not used concurrently by different service workers.
- After the data fetching cycle is complete, all proxies are unlocked (released) so that they can be reused for subsequent cycles or by other projects.

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
Proxies are loaded using [FastAPI-Proxy-API](https://github.com/pudjojotaro/fastapi-proxy-api) proxy api. The application sends a request to get all available proxies, constructs them into addresses and after being complete with the cycle sends a request to unlock them so that other projects can use the proxies. 

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
| telegram-alert-bot | Async Telegram alert system | 0.2.0 | [telegram-alert-bot](https://github.com/pudjojotaro/telegram-alert-bot) |
| asyncio | Async I/O framework | Built-in | [asyncio Docs](https://docs.python.org/3/library/asyncio.html) |
| logging | Logging facility | Built-in | [logging Docs](https://docs.python.org/3/library/logging.html) |
| proxy-api | Proxy Management | Latest | [FastAPI-Proxy-API](https://github.com/pudjojotaro/fastapi-proxy-api) |

---

## License
This project is licensed under:
```
MIT License
```
