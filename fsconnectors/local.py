import datetime
import os
import shutil
from typing import IO, Any

from fsconnectors.connector import Connector
from fsconnectors.utils.entry import FSEntry


class LocalConnector(Connector):
    """Local file system connector."""

    def open(self, path: str, mode: str = 'r') -> IO[Any]:
        return open(path, mode)

    def mkdir(self, path: str) -> None:
        os.makedirs(path, exist_ok=True)

    def copy(self, src_path: str, dst_path: str, recursive: bool = False) -> None:
        if not os.path.exists(src_path):
            raise FileNotFoundError(f"No such file or directory: '{src_path}'")
        elif os.path.isdir(src_path) and recursive:
            shutil.copytree(src_path, dst_path)
        elif os.path.isfile(src_path):
            shutil.copyfile(src_path, dst_path)
        else:
            raise ValueError(f"'{src_path}' is a directory, but recursive mode is disabled")

    def move(self, src_path: str, dst_path: str, recursive: bool = False) -> None:
        if not os.path.exists(src_path):
            raise FileNotFoundError(f"No such file or directory: '{src_path}'")
        elif (os.path.isdir(src_path) and recursive) or os.path.isfile(src_path):
            os.rename(src_path, dst_path)
        else:
            raise ValueError(f"'{src_path}' is a directory, but recursive mode is disabled")

    def remove(self, path: str, recursive: bool = False) -> None:
        if not os.path.exists(path):
            raise FileNotFoundError(f"No such file or directory: '{path}'")
        elif os.path.isdir(path) and recursive:
            shutil.rmtree(path)
        elif os.path.isfile(path):
            os.remove(path)
        else:
            raise ValueError(f"'{path}' is a directory, but recursive mode is disabled")

    def listdir(self, path: str, recursive: bool = False) -> list[str]:
        if recursive:
            result = []
            for root, dirs, files in os.walk(path):
                for name in files:
                    result.append(os.path.join(root, name))
                for name in dirs:
                    result.append(os.path.join(root, name))
            return result
        else:
            return os.listdir(path)

    def scandir(self, path: str, recursive: bool = False) -> list[FSEntry]:
        result = []
        if recursive:
            for root, dirs, files in os.walk(path):
                for name in dirs:
                    result.append(FSEntry(name, os.path.join(root, name), 'dir'))
                for name in files:
                    path = os.path.join(root, name)
                    size = os.path.getsize(path)
                    last_modified = datetime.datetime.fromtimestamp(os.path.getmtime(path))
                    result.append(FSEntry(name, path, 'file', size, last_modified))
        else:
            for entry in os.scandir(path):
                if entry.is_dir():
                    result.append(FSEntry(entry.name, entry.path, 'dir'))
                elif entry.is_file():
                    size = entry.stat().st_size
                    last_modified = datetime.datetime.fromtimestamp(entry.stat().st_mtime)
                    result.append(FSEntry(entry.name, entry.path, 'file', size, last_modified))
        return result
