import datetime
from dataclasses import dataclass
from typing import Optional, Literal


@dataclass
class FSEntry:
    name: str
    path: str
    type: Literal['file', 'dir']
    size: Optional[int] = None
    last_modified: Optional[datetime.datetime] = None
