import datetime
from dataclasses import dataclass
from typing import Literal, Optional


@dataclass
class FSEntry:
    name: str
    path: str
    type: Literal['file', 'dir']
    size: Optional[int] = None
    last_modified: Optional[datetime.datetime] = None
