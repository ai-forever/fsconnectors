from typing import List, IO
from abc import ABC, abstractmethod
from contextlib import contextmanager

from fsconnectors.utils.entry import FSEntry


class Connector(ABC):
    """Abstract class for connector"""

    @abstractmethod
    @contextmanager
    def open(self, path: str, mode: str) -> IO:
        pass

    @abstractmethod
    def mkdir(self, path: str):
        pass

    @abstractmethod
    def copy(self, src_path: str, dst_path: str, recursive: bool):
        pass

    @abstractmethod
    def move(self, src_path: str, dst_path: str, recursive: bool):
        pass

    @abstractmethod
    def remove(self, path: str, recursive: bool):
        pass

    @abstractmethod
    def listdir(self, path: str, recursive: bool) -> List[str]:
        pass

    @abstractmethod
    def scandir(self, path: str, recursive: bool) -> List[FSEntry]:
        pass
