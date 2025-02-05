import pandas as pd
from bs4 import BeautifulSoup
from ..config.constants import ALLOWED_GEMS_PRISMATIC, ALLOWED_GEMS_ETHEREAL, COURIERS, ITEMS
import logging
import re
import math

def parse_market_listings(market_listings):
    logger = logging.getLogger('parsing')
    logger.info(f"Starting to parse {len(market_listings)} market listings")
    data = []
    
    for listing in market_listings:
        try:
            item_description = listing.item.description.market_name
            logger.info(f"Processing listing: {item_description}")
            
            listing_data = {
                "ID": listing.id,
                "Price": listing.converted_price / 100 + listing.converted_fee/100,
                "Item Description": item_description,
                "Ethereal Gem": None,
                "Prismatic Gem": None,
            }

            if item_description in COURIERS:
                ethereal_gem = None
                prismatic_gem = None
                
                for desc in listing.item.description.descriptions:
                    if "Gem" in desc.value:
                        soup = BeautifulSoup(desc.value, 'html.parser')
                        gem_text = soup.get_text(strip=True).replace("Empty Socket", "").strip()
                        logger.info(f"Gem text: {gem_text}")
                        logger.info(f"Price: {listing.converted_price / 100 + listing.converted_fee/100}")
                        logger.info(f"ID: {listing.id}")
                        logger.info(f"Item Description: {item_description}")

                        print(f"Gem text: {gem_text}")
                        print(listing.converted_price / 100 + listing.converted_fee/100)
                        print(listing.id)
                        print(item_description)
                        
                        ethereal_gem, prismatic_gem = parse_gem_text_both_gems(gem_text)
                        print(f"Ethereal Gem: {ethereal_gem}")
                        print(f"Prismatic Gem: {prismatic_gem}")
                
                listing_data["Ethereal Gem"] = ethereal_gem
                listing_data["Prismatic Gem"] = prismatic_gem

            else:
                for desc in listing.item.description.descriptions:
                    if "Gem" in desc.value:
                        soup = BeautifulSoup(desc.value, 'html.parser')
                        gem_text = soup.get_text(strip=True).replace("Empty Socket", "").strip()

                        if ("Swine of the Sunken Galley" in item_description and "Explosive Burst" in gem_text) or \
                           ("Fractal Horns of Inner Abysm" in item_description and "Reflection's Shade" in gem_text):
                            listing_data["Prismatic Gem"] = None
                        else:
                            matching_gem = next((allowed for allowed in ALLOWED_GEMS_PRISMATIC if allowed in gem_text), None)
                            if matching_gem:
                                listing_data["Prismatic Gem"] = matching_gem
                        break

            if listing_data["Ethereal Gem"] or listing_data["Prismatic Gem"]:
                logger.info(f"Found item with gems: {item_description} - E: {listing_data['Ethereal Gem']}, P: {listing_data['Prismatic Gem']}")
                data.append(listing_data)
        except Exception as e:
            logger.error(f"Error processing listing {listing.id}: {str(e)}", exc_info=True)
    
    logger.info(f"Finished parsing. Found {len(data)} items with gems")
    return pd.DataFrame(data)


def parse_gem_text_both_gems(gem_text):
    ethereal_gem = None
    prismatic_gem = None

    # Determine which gem type appears first using regex
    prismatic_match = re.search(r'(.*?)Prismatic Gem', gem_text)
    ethereal_match = re.search(r'(.*?)Ethereal Gem', gem_text)

    if prismatic_match and (not ethereal_match or prismatic_match.start() < ethereal_match.start()):
        # Process Prismatic Gem first
        prismatic_name = prismatic_match.group(1).strip()
        if prismatic_name in ALLOWED_GEMS_PRISMATIC:
            prismatic_gem = prismatic_name
        # Remove processed gem from text
        gem_text = gem_text[prismatic_match.end():].strip()

    elif ethereal_match:
        # Process Ethereal Gem first
        ethereal_name = ethereal_match.group(1).strip()
        if ethereal_name in ALLOWED_GEMS_ETHEREAL:
            ethereal_gem = ethereal_name
        # Remove processed gem from text
        gem_text = gem_text[ethereal_match.end():].strip()

    # Process the remaining gem type
    prismatic_match = re.search(r'(.*?)Prismatic Gem', gem_text)
    if prismatic_match:
        prismatic_name = prismatic_match.group(1).strip()
        if prismatic_name in ALLOWED_GEMS_PRISMATIC:
            prismatic_gem = prismatic_name

    ethereal_match = re.search(r'(.*?)Ethereal Gem', gem_text)
    if ethereal_match:
        ethereal_name = ethereal_match.group(1).strip()
        if ethereal_name in ALLOWED_GEMS_ETHEREAL:
            ethereal_gem = ethereal_name

    # Debugging output
    print(f"Processed Gem Text: {gem_text}")
    print(f"Matched Ethereal Gem: {ethereal_gem}")
    print(f"Matched Prismatic Gem: {prismatic_gem}")
    print("-" * 50)

    return ethereal_gem, prismatic_gem


def _robust_parse_price(raw_price: str) -> float:
    """
    First try a direct float parse. If it fails, strip out anything that's not
    digit, decimal point, or minus sign, and parse again. Divide final float by 100 
    (to match your existing logic).
    """
    try:
        return float(raw_price) / 100
    except ValueError:
        logging.warning(f"Could not parse price '{raw_price}', attempting fallback cleanup.")
        cleaned = re.sub(r'[^0-9.-]', '', raw_price)
        return float(cleaned) / 100

def _robust_parse_quantity(raw_quantity: str) -> int:
    """
    First try to parse as int. If it fails, try parsing as float and floor it.
    """
    try:
        return int(raw_quantity)
    except ValueError:
        try:
            # Handle decimal numbers by converting to float first
            return math.floor(float(raw_quantity))
        except ValueError:
            logging.warning(f"Could not parse quantity '{raw_quantity}', cleaning and retrying")
            cleaned = re.sub(r'[^0-9.-]', '', raw_quantity)
            return math.floor(float(cleaned))

def process_histogram(histogram):
    """
    Original logic for processing histogram buy orders,
    extended to handle unexpected strings more gracefully.
    """
    logger = logging.getLogger('parsing')

    # Empty or None histogram?
    if not histogram:
        return {
            "buy_orders": [],
            "buy_order_length": 0,
        }

    # Check if 'buy_order_graph' exists
    if not hasattr(histogram, 'buy_order_graph'):
        logger.warning("Histogram missing buy_order_graph attribute")
        logger.debug(f"Raw histogram data: {histogram}")
        return {
            "buy_orders": [],
            "buy_order_length": 0,
        }

    try:
        buy_orders = []
        logger.debug(f"Processing buy_order_graph: {histogram.buy_order_graph}")

        # Process each buy order entry
        for entry in histogram.buy_order_graph:
            try:
                # Convert price
                price_str = str(entry.price).replace(',', '').strip()
                price = _robust_parse_price(price_str)

                # Convert quantity
                quantity_str = str(entry.quantity).replace(',', '').strip()
                quantity = _robust_parse_quantity(quantity_str)

                buy_orders.append([price, quantity])

            except (ValueError, AttributeError) as e:
                # Only skip this one malformed entry
                logger.warning(f"Skipping malformed buy order entry {entry}: {e}")
                continue

        # If no buy orders, return empty
        if not buy_orders:
            return {
                "buy_orders": [],
                "buy_order_length": 0,
            }
        
        # Convert cumulative data to incremental:
        for i in range(len(buy_orders) - 1, 0, -1):
            current_quantity = buy_orders[i][1]
            previous_quantity = buy_orders[i - 1][1]
            # Subtract the previous from the current to get incremental quantity
            buy_orders[i][1] = current_quantity - previous_quantity
        
        buy_order_length = sum(order[1] for order in buy_orders if order[1] > 0)
        
        return {
            "buy_orders": buy_orders,
            "buy_order_length": buy_order_length,
        }
    except Exception as e:
        # Catch-all for any unexpected failures
        logger.error(f"Error processing histogram: {str(e)}")
        logger.error(f"Raw histogram data: {histogram}")
        if hasattr(histogram, 'buy_order_graph'):
            logger.error(f"Buy order graph: {histogram.buy_order_graph}")
        return {
            "buy_orders": [],
            "buy_order_length": 0,
        }
