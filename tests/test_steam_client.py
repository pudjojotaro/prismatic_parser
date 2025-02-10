import asyncio
import logging
import os
import sys

# Add the project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.utils.steam_client import SteamMarketClient

async def test_steam_client():
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('test_steam_client')
    
    try:
        # Initialize the client
        logger.info("Initializing Steam client...")
        client = SteamMarketClient()
        
        # Test config loading
        logger.info("Testing config loading...")
        config = client.load_config()
        logger.info(f"Config loaded successfully: {config.keys()}")
        
        # Test client initialization
        logger.info("Testing full client initialization...")
        await client.initialize()
        logger.info("Client initialized successfully!")
        
        # Test session status
        if client.client:
            is_alive = await client.client.is_session_alive()
            logger.info(f"Session is alive: {is_alive}")
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}", exc_info=True)
    finally:
        if client and client.client:
            logger.info("Closing client...")
            await client.close()
            logger.info("Client closed.")

if __name__ == "__main__":
    asyncio.run(test_steam_client()) 