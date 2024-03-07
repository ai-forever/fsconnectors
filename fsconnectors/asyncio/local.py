import os
import datetime
import aiofiles
import aioshutil
import aiofiles.os
from typing import List, Any
from contextlib import asynccontextmanager

from fsconnectors.utils.entry import FSEntry
from fsconnectors.asyncio.connector import AsyncConnector


class AsyncLocalConnector(AsyncConnector):
    """Async local file system connector."""

    @classmethod
    @asynccontextmanager
    async def connect(cls) -> 'AsyncLocalConnector':
        """Connects to file system.

        Yields
        -------
        AsyncLocalConnector
            Class instance
        """
        yield cls()

    @asynccontextmanager
    async def open(self, path: str, mode: str = 'r') -> Any:
        async with aiofiles.open(path, mode) as f:
            yield f

    async def mkdir(self, path: str) -> None:
        await aiofiles.os.makedirs(path, exist_ok=True)

    async def copy(self, src_path: str, dst_path: str, recursive: bool = False) -> None:
        if not await aiofiles.os.path.exists(src_path):
            raise FileNotFoundError(f"No such file or directory: '{src_path}'")
        elif await aiofiles.os.path.isdir(src_path) and recursive:
            await aioshutil.copytree(src_path, dst_path)
        elif await aiofiles.os.path.isfile(src_path):
            await aioshutil.copyfile(src_path, dst_path)
        else:
            raise ValueError(f"'{src_path}' is a directory, but recursive mode is disabled")

    async def move(self, src_path: str, dst_path: str, recursive: bool = False) -> None:
        if not await aiofiles.os.path.exists(src_path):
            raise FileNotFoundError(f"No such file or directory: '{src_path}'")
        elif (await aiofiles.os.path.isdir(src_path) and recursive) or await aiofiles.os.path.isfile(src_path):
            await aiofiles.os.rename(src_path, dst_path)
        else:
            raise ValueError(f"'{src_path}' is a directory, but recursive mode is disabled")

    async def remove(self, path: str, recursive: bool = False) -> None:
        if not await aiofiles.os.path.exists(path):
            raise FileNotFoundError(f"No such file or directory: '{path}'")
        elif await aiofiles.os.path.isdir(path) and recursive:
            await aioshutil.rmtree(path)
        elif await aiofiles.os.path.isfile(path):
            await aiofiles.os.remove(path)
        else:
            raise ValueError(f"'{path}' is a directory, but recursive mode is disabled")

    async def listdir(self, path: str, recursive: bool = False) -> List[str]:
        if recursive:
            result = []
            for root, dirs, files in os.walk(path):  # TODO: async
                for name in files:
                    result.append(os.path.join(root, name))
                for name in dirs:
                    result.append(os.path.join(root, name))
            return result
        else:
            result = await aiofiles.os.listdir(path)
            return result

    async def scandir(self, path: str, recursive: bool = False) -> List[FSEntry]:
        result = []
        if recursive:
            for root, dirs, files in os.walk(path):  # TODO: async
                for name in dirs:
                    result.append(FSEntry(name, os.path.join(root, name), 'dir'))
                for name in files:
                    path = os.path.join(root, name)
                    size = await aiofiles.os.path.getsize(path)
                    last_modified = datetime.datetime.fromtimestamp(await aiofiles.os.path.getmtime(path))
                    result.append(FSEntry(name, path, 'file', size, last_modified))
        else:
            async for entry in aiofiles.os.scandir(path):
                if entry.is_dir():
                    result.append(FSEntry(entry.name, entry.path, 'dir'))
                elif entry.is_file():
                    size = entry.stat().st_size
                    last_modified = datetime.datetime.fromtimestamp(entry.stat().st_mtime)
                    result.append(FSEntry(entry.name, entry.path, 'file', size, last_modified))
        return result
