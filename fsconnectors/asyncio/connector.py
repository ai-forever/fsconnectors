from typing import List, IO
from abc import abstractmethod
from contextlib import asynccontextmanager

from fsconnectors.connector import Connector
from fsconnectors.utils.entry import FSEntry


class AsyncConnector(Connector):
    """Abstract class for async connector"""

    @classmethod
    @abstractmethod
    @asynccontextmanager
    async def connect(cls) -> 'AsyncConnector':
        pass

    @abstractmethod
    @asynccontextmanager
    async def open(self, path: str, mode: str) -> IO:
        pass

    @abstractmethod
    async def mkdir(self, path: str):
        pass

    @abstractmethod
    async def copy(self, src_path: str, dst_path: str, recursive: bool = False):
        pass

    @abstractmethod
    async def move(self, src_path: str, dst_path: str, recursive: bool = False):
        pass

    @abstractmethod
    async def remove(self, path: str, recursive: bool = False):
        pass

    @abstractmethod
    async def listdir(self, path: str, recursive: bool = False) -> List[str]:
        pass

    @abstractmethod
    async def scandir(self, path: str, recursive: bool = False) -> List[FSEntry]:
        pass
