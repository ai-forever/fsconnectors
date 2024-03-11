from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from typing import Any

from fsconnectors.utils.entry import FSEntry


class Connector(ABC):
    """Abstract class for connector."""

    @abstractmethod
    def open(self, path: str, mode: str) -> AbstractContextManager[Any]:
        """Open file.

        Parameters
        ----------
        path : str
            Path to file.
        mode : str
            Open mode.

        Returns
        -------
        Any
            Readable/writable file-like object.
        """
        pass

    @abstractmethod
    def mkdir(self, path: str) -> None:
        """Make directory.

        Parameters
        ----------
        path : str
            Directory path.
        """
        pass

    @abstractmethod
    def copy(self, src_path: str, dst_path: str, recursive: bool = False) -> None:
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
    def move(self, src_path: str, dst_path: str, recursive: bool = False) -> None:
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
    def remove(self, path: str, recursive: bool = False) -> None:
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
    def listdir(self, path: str, recursive: bool = False) -> list[str]:
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
    def scandir(self, path: str, recursive: bool = False) -> list[FSEntry]:
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
