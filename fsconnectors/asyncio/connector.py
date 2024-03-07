from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, AsyncIterator, List

from fsconnectors.utils.entry import FSEntry


class AsyncConnector(ABC):
    """Abstract class for async connector."""

    @asynccontextmanager
    async def connect(self) -> AsyncGenerator['AsyncConnector', None]:
        """Connects to file system.

        Yields
        -------
        AsyncConnector
            Class instance
        """
        yield self

    @abstractmethod
    @asynccontextmanager
    async def open(self, path: str, mode: str) -> AsyncIterator[Any]:
        yield None

    @abstractmethod
    async def mkdir(self, path: str) -> None:
        pass

    @abstractmethod
    async def copy(self, src_path: str, dst_path: str, recursive: bool = False) -> None:
        pass

    @abstractmethod
    async def move(self, src_path: str, dst_path: str, recursive: bool = False) -> None:
        pass

    @abstractmethod
    async def remove(self, path: str, recursive: bool = False) -> None:
        pass

    @abstractmethod
    async def listdir(self, path: str, recursive: bool = False) -> List[str]:
        pass

    @abstractmethod
    async def scandir(self, path: str, recursive: bool = False) -> List[FSEntry]:
        pass
