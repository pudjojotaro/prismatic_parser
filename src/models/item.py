from dataclasses import dataclass
from typing import Optional

@dataclass
class Item:
    id: str
    name: str
    price: float
    ethereal_gem: Optional[str] = None
    prismatic_gem: Optional[str] = None
    timestamp: Optional[float] = None
