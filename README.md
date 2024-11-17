
# Courier Check Public Client

![Project Banner](./images/github-header-image.png)  
![Python](https://img.shields.io/badge/python-3.8%2B-blue) 
![License](https://img.shields.io/badge/license-MIT-green) 
![Status](https://img.shields.io/badge/status-Demonstration-orange)  
![Version](https://img.shields.io/badge/version-1.0-blue)
![Platform](https://img.shields.io/badge/platform-Windows%20|%20MacOS-lightgrey)

## Introduction
The **Courier Check Public Client** is a demonstration project designed to analyze the profitability of items and gems in Steam's marketplace. The project fetches market data using asynchronous proxy workers and employs controlled delays to avoid rate limits. The analysis focuses on comparing item prices with the combined prices of associated gems to identify profitable trading opportunities.

---

## Table of Contents
- [Introduction](#introduction)
- [Features](#features)
- [Project Structure](#project-structure)
- [Design and Structure](#design-and-structure)
- [Database Schema](#database-schema)
- [Profitability Comparison](#profitability-comparison)
- [Configuration](#configuration)
- [Technology Stack](#technology-stack)
- [License](#license)

---

## Features
| Feature                          | Description                                                                 |
|----------------------------------|-----------------------------------------------------------------------------|
| **Asynchronous Proxy Workers**   | Fetches listings using multiple proxies for improved efficiency.            |
| **Rate Limit Avoidance**         | Incorporates controlled delays and waits to comply with API limits.         |
| **Profitability Analysis**       | Compares item prices against combined gem prices to calculate potential profit. |
| **Historical Tracking**          | Maintains timestamps for historical trend analysis.                         |

---

## Project Structure

```plaintext
root/
├── proxies/
│   ├── proxies_items.txt         # Proxy list for fetching items
│   ├── proxies_gems.txt          # Proxy list for fetching gems
├── python_helpers/
│   ├── gems_ethereal_with_ID.json # Ethereal gem data with IDs
│   ├── gems_prismatic_with_ID.json # Prismatic gem data with IDs
├── database.db                   # SQLite database file
├── requirements.txt              # Python dependencies
├── main.py                       # Main script for market analysis
├── fetch_gems.py                 # Script for fetching gem data
├── monitor.py                    # Script for monitoring profitability
├── README.md                     # Project documentation
```

---

## Design and Structure

### Core Components
1. **Proxy Workers**:
   - Uses a pool of proxies to fetch data asynchronously.
   - Randomized wait times between requests ensure compliance with rate limits.

2. **Database Schema**:
   - Stores data about items, gems, and profitability comparisons (detailed below).

3. **Comparison Engine**:
   - Calculates profitability by comparing item prices with combined gem prices.
   - Uses buy order data for the most accurate analysis.

4. **Data Processing**:
   - Processes market data using Python libraries like `pandas` and `sqlite3`.

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

## Profitability Comparison

### How It Works

To determine profitability, the system compares the price of an item with the combined prices of the associated prismatic and ethereal gems:

1. **Extract Buy Orders**:
   - For both prismatic and ethereal gems, the highest buy order is retrieved from their respective buy orders list.

   - Let:
     - \( P_{	ext{prismatic}} \) = highest prismatic gem buy order.
     - \( P_{	ext{ethereal}} \) = highest ethereal gem buy order.

2. **Calculate Combined Gem Price**:
   - The combined gem price is calculated as:
     \[
     P_{	ext{combined}} = P_{	ext{prismatic}} + P_{	ext{ethereal}}
     \]

3. **Profitability Check**:
   - An item is considered profitable if:
     \[
     P_{	ext{item}} < P_{	ext{combined}} 	imes (1 - 	ext{SteamFee} - 	ext{TargetProfit})
     \]
     where:
     - \( P_{	ext{item}} \) is the item price.
     - \( 	ext{SteamFee} \) is the Steam transaction fee percentage.
     - \( 	ext{TargetProfit} \) is the desired profit margin.

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

| Technology | Description                                         | Links                     |
|------------|-----------------------------------------------------|---------------------------|
| Python     | Core programming language used for the project.     | [Python.org](https://www.python.org/) |
| SQLite     | Lightweight, file-based database for storage.       | [SQLite.org](https://sqlite.org/index.html) |
| Pandas     | Data manipulation and analysis library.             | [Pandas Docs](https://pandas.pydata.org/) |
| BeautifulSoup | HTML and XML parsing library.                     | [BeautifulSoup Docs](https://www.crummy.com/software/BeautifulSoup/) |
| AioSteamPy | Asynchronous Steam API client library.              | [AioSteamPy](https://pypi.org/project/aiosteampy/) |

---

## License
This project is licensed under:
```
MIT License
```
