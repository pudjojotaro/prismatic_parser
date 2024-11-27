import logging
from telegram_alert_bot import TelegramAlertBot
from ..config.settings import settings
from ..models.item import Item
from ..models.comparison import Comparison
from datetime import datetime
from ..utils.messages import Messages

class AlertService:
    def __init__(self):
        self.telegram_bot = TelegramAlertBot(
            token=settings.BOT_TOKEN,
            user_id=settings.CHAT_ID,
            merge_pattern="💤 No profitable items found between"
        )
        
    async def send_startup_message(self):
        await self.telegram_bot.event_trigger(Messages.START, "Prismatic_Parser_Bot")
        
    async def send_shutdown_message(self):
        await self.telegram_bot.event_trigger(Messages.SHUTDOWN, "Prismatic_Parser_Bot")
        
    async def send_no_profit_alert(self, fetch_start: float, fetch_end: float):
        start_time = datetime.fromtimestamp(fetch_start).strftime('%b %d %H:%M:%S')
        end_time = datetime.fromtimestamp(fetch_end).strftime('%b %d %H:%M:%S')
        message = Messages.NO_PROFIT.format(start_time, end_time)
        await self.telegram_bot.event_trigger(message, "Prismatic_Parser_Bot")
        
    async def send_profit_alert(self, item: Item, comparison: Comparison):
        message = Messages.PROFIT_FOUND.format(
            item.name,
            comparison.item_price,
            comparison.combined_gem_price,
            comparison.expected_profit,
            comparison.item_id
        )
        await self.telegram_bot.event_trigger(message, "Prismatic_Parser_Bot")
        
    async def run_bot(self):
        await self.telegram_bot.background_bot_polling()
