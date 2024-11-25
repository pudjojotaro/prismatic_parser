class Messages:
    # App Status
    START = "🚀 Prismatic Parser Bot Starting!\n📡 Initializing services...\n🔌 Connecting to API\n✅ Ready to monitor items"
    SHUTDOWN = "🛑 Prismatic Parser Bot Shutting Down\n✅ Services stopped gracefully\n👋 Goodbye!"
    
    # Monitoring Messages
    COMPARISON_START = "\n📊 === Starting comparison for item ==="
    ITEM_DETAILS = "📦 Item ID: {}\n📝 Item Name: {}\n💰 Item Price: {}"
    TIMESTAMP = "⏰ Item Timestamp: {}"
    
    # Gem Details
    ETHEREAL_GEM = "\n💫 Ethereal Gem: {}"
    PRISMATIC_GEM = "\n🌈 Prismatic Gem: {}"
    GEM_LAST_UPDATED = "🕒 {} Gem Last Updated: {}"
    GEM_TOP_ORDERS = "📈 {} Gem Top 3 Buy Orders:"
    GEM_ORDER_ENTRY = "  {}. 💰 Price: {:.2f}, 📦 Quantity: {}"
    GEM_NOT_FOUND = "⚠️ {} Gem not found in database"
    
    # Processing Messages
    FETCH_START = "🔄 Fetching histogram"
    PARSE_SUCCESS = "✅ Successfully parsed histogram: {} buy orders"
    NO_ORDERS = "ℹ️ No buy orders found in histogram"
    ITEMS_FOUND = "✨ Found {} items with gems"
    
    # Profit Alerts
    NO_PROFIT = "💤 No profitable items found between {} and {}"
    PROFIT_FOUND = """💰 Profitable item found!
    📦 Name: {}
    💵 Price: {}
    💎 Combined gem price: {}
    📈 Profit: {:.2f}
    🔑 ID: {}""" 