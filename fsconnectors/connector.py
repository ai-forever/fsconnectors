from typing import List, IO
from abc import ABC, abstractmethod
from contextlib import contextmanager

from fsconnectors.utils.entry import FSEntry


class Connector(ABC):
    """Abstract class for connector"""

    @abstractmethod
    @contextmanager
    def open(self, path: str, mode: str) -> IO:
        """Open file"""
        pass

    @abstractmethod
    def mkdir(self, path: str):
        """Make directory"""
        pass

    @abstractmethod
    def copy(self, src_path: str, dst_path: str, recursive: bool = False):
        """Copy file or directory"""
        pass

    @abstractmethod
    def move(self, src_path: str, dst_path: str, recursive: bool = False):
        """Move file or directory"""
        pass

    @abstractmethod
    def remove(self, path: str, recursive: bool = False):
        """Delete file or directory"""
        pass

    @abstractmethod
    def listdir(self, path: str, recursive: bool = False) -> List[str]:
        """List directory content"""
        pass

    @abstractmethod
    def scandir(self, path: str, recursive: bool = False) -> List[FSEntry]:
        """List directory content with metadata"""
        pass
