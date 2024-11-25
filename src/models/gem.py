from dataclasses import dataclass
from typing import List

@dataclass
class Gem:
    name: str
    buy_orders: str
    buy_order_length: int
    timestamp: float
    
    @property
    def parsed_buy_orders(self) -> List[List[float]]:
        return eval(self.buy_orders)
