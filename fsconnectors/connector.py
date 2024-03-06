from typing import List, IO
from abc import ABC, abstractmethod
from contextlib import contextmanager

from fsconnectors.utils.entry import FSEntry


class Connector(ABC):
    """Abstract class for connector."""

    @abstractmethod
    @contextmanager
    def open(self, path: str, mode: str) -> IO:
        """Open file.

        Parameters
        ----------
        path : str
            Path to file.
        mode : str
            Open mode.

        Returns
        -------
        IO
            File-like object.
        """
        pass

    @abstractmethod
    def mkdir(self, path: str):
        """Make directory.

        Parameters
        ----------
        path : str
            Directory path.
        """
        pass

    @abstractmethod
    def copy(self, src_path: str, dst_path: str, recursive: bool = False):
        """Copy file or directory.

        Parameters
        ----------
        src_path : str
            Source path.
        dst_path : str
            Destination path.
        recursive : bool, default=False
            Recursive.
        """
        pass

    @abstractmethod
    def move(self, src_path: str, dst_path: str, recursive: bool = False):
        """Move file or directory.

        Parameters
        ----------
        src_path : str
            Source path.
        dst_path : str
            Destination path.
        recursive : bool, default=False
            Recursive.
        """
        pass

    @abstractmethod
    def remove(self, path: str, recursive: bool = False):
        """Delete file or directory.

        Parameters
        ----------
        path : str
            File or directory path.
        recursive : bool, default=False
            Recursive.
        """
        pass

    @abstractmethod
    def listdir(self, path: str, recursive: bool = False) -> List[str]:
        """List directory content.

        Parameters
        ----------
        path : str
            Directory path.
        recursive : bool, default=False
            Recursive.

        Returns
        -------
        List[str]
            List of directory contents.
        """
        pass

    @abstractmethod
    def scandir(self, path: str, recursive: bool = False) -> List[FSEntry]:
        """List directory content with metadata.

        Parameters
        ----------
        path : str
            Directory path.
        recursive : bool, default=False
            Recursive.

        Returns
        -------
        List[FSEntry]
            List of directory contents with metadata.
        """
        pass
