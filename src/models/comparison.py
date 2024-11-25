from dataclasses import dataclass
from typing import Optional

@dataclass
class Comparison:
    item_id: str
    item_price: float
    is_profitable: bool
    timestamp: float
    prismatic_gem_price: Optional[float] = None
    ethereal_gem_price: Optional[float] = None
    combined_gem_price: Optional[float] = None
    expected_profit: Optional[float] = None
