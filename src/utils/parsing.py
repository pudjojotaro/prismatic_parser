import pandas as pd
from bs4 import BeautifulSoup
from ..config.constants import ALLOWED_GEMS_PRISMATIC, ALLOWED_GEMS_ETHEREAL, COURIERS, ITEMS
import logging
import re

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



def process_histogram(histogram_data):
    """Process the histogram data from Steam API.
    
    Note: We ignore sell order parsing errors as we only care about buy orders.
    Some high-value items (4+ digit prices) may cause parsing issues in sell orders
    but this doesn't affect our buy order processing.
    """
    try:
        if not histogram_data or 'buy_order_graph' not in histogram_data:
            return None

        buy_orders = []
        buy_order_graph = histogram_data.get('buy_order_graph', [])
        
        for price_point in buy_order_graph:
            if len(price_point) >= 2:
                price = float(price_point[0])
                quantity = int(price_point[1])
                buy_orders.append([price, quantity])
        
        # Even if sell_order_graph parsing fails, we don't care
        # Just log a warning if there's an issue
        try:
            sell_order_graph = histogram_data.get('sell_order_graph', [])
            for price_point in sell_order_graph:
                price = float(price_point[0])  # This might fail for high values
        except ValueError as e:
            logging.warning(f"Ignoring sell order parsing error (expected for high-value items): {e}")
        
        return {
            "buy_orders": buy_orders,
            "buy_order_length": len(buy_orders)
        }
        
    except Exception as e:
        logging.error(f"Error processing histogram: {e}")
        return None
