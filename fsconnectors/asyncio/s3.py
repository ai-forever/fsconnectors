from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import IO, Any, Union

import aioboto3
import yaml

from fsconnectors.asyncio.connector import AsyncConnector
from fsconnectors.utils.entry import FSEntry
from fsconnectors.utils.s3 import (
    AsyncMultipartWriter,
    AsyncS3Reader,
    AsyncSinglepartWriter,
)


class AsyncS3Connector(AsyncConnector):
    """Async S3 connector.

    Attributes
    ----------
    endpoint_url : str
        Endpoint URL.
    aws_access_key_id : str
        AWS access key ID
    aws_secret_access_key : str
        AWS secret access key
    """

    def __init__(
        self,
        endpoint_url: str,
        aws_access_key_id: str,
        aws_secret_access_key: str
    ) -> None:
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.client: Any = None

    @asynccontextmanager
    async def connect(self) -> AsyncGenerator['AsyncS3Connector', None]:
        """Connects to file system.

        Yields
        -------
        AsyncS3Connector
            Class instance
        """
        async with aioboto3.Session().client(
                's3', endpoint_url=self.endpoint_url,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key
        ) as client:
            self.client = client
            yield self

    @classmethod
    def from_yaml(cls, path: str) -> 'AsyncS3Connector':
        """Creates class instance from configuration path.

        Parameters
        ----------
        path : str
            path to configuration file.

        Returns
        -------
        AsyncS3Connector
            Class instance.
        """
        with open(path) as f:
            config = yaml.safe_load(f)
        return cls(**config)

    def open(
        self,
        path: str,
        mode: str = 'rb',
        multipart: bool = False
    ) -> Union[AsyncS3Reader, AsyncMultipartWriter, AsyncSinglepartWriter]:
        """Open file

        Parameters
        ----------
        path : str
            Path to file.
        mode : str
            Open mode.
        multipart : bool, default=False
            Use multipart writer.

        Returns
        -------
        Union[AsyncS3Reader, AsyncMultipartWriter, AsyncSinglepartWriter]
            Readable/writable file-like object.
        """
        stream: Union[AsyncS3Reader, AsyncMultipartWriter, AsyncSinglepartWriter]
        bucket, key = self._split_path(path)
        if mode in ['r', 'rb', 'rt']:
            stream = AsyncS3Reader(self.client, bucket=bucket, key=key, mode=mode)
        elif mode in ['w', 'wb', 'wt']:
            if multipart:
                stream = AsyncMultipartWriter(self.client, bucket=bucket, key=key, mode=mode)
            else:
                stream = AsyncSinglepartWriter(self.client, bucket=bucket, key=key, mode=mode)
        else:
            raise ValueError(f"invalid mode: '{mode}'")
        return stream

    async def mkdir(self, path: str) -> None:
        path = path.rstrip('/') + '/'
        bucket, key = self._split_path(path)
        await self.client.put_object(Bucket=bucket, Key=key)

    async def copy(self, src_path: str, dst_path: str, recursive: bool = False) -> None:
        if recursive:
            src_path = src_path.rstrip('/') + '/'
            dst_path = dst_path.rstrip('/') + '/'
            src_bucket, src_key = self._split_path(src_path)
            dst_bucket, dst_key = self._split_path(dst_path)
            paths = await self.listdir(src_path, recursive)
            for path in paths:
                path_bucket, path_key = self._split_path(path)
                await self.client.copy({'Bucket': path_bucket, 'Key': path_key},
                                       dst_bucket, path_key.replace(src_key, dst_key))
        else:
            src_bucket, src_key = self._split_path(src_path)
            dst_bucket, dst_key = self._split_path(dst_path)
            await self.client.copy({'Bucket': src_bucket, 'Key': src_key}, dst_bucket, dst_key)

    async def move(self, src_path: str, dst_path: str, recursive: bool = False) -> None:
        await self.copy(src_path, dst_path, recursive)
        await self.remove(src_path, recursive)

    async def remove(self, path: str, recursive: bool = False) -> None:
        if recursive:
            path = path.rstrip('/') + '/'
            paths = await self.listdir(path, recursive)
            for path in paths:
                path_bucket, path_key = self._split_path(path)
                await self.client.delete_object(Bucket=path_bucket, Key=path_key)
        else:
            bucket, key = self._split_path(path)
            await self.client.delete_object(Bucket=bucket, Key=key)

    async def listdir(self, path: str, recursive: bool = False, dirs: bool = False) -> list[str]:
        entries = await self.scandir(path, recursive, dirs)
        if recursive:
            result = [entry.path for entry in entries]
        else:
            result = [entry.name for entry in entries]
        return result

    async def scandir(self, path: str, recursive: bool = False, dirs: bool = False) -> list[FSEntry]:
        result = []
        path = path.rstrip('/') + '/'
        bucket, prefix = self._split_path(path)
        paginator = self.client.get_paginator('list_objects')
        if recursive:
            paginator_result = paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={'PageSize': 1000})
        else:
            paginator_result = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/',
                                                  PaginationConfig={'PageSize': 1000})
        async for item in paginator_result.search('Contents'):
            if item:
                path = bucket + '/' + item.get('Key')
                name = path.split('/')[-1]
                if name:
                    size = item.get('Size')
                    last_modified = item.get('LastModified')
                    result.append(FSEntry(name, path, 'file', size, last_modified))
        if dirs:
            paginator = self.client.get_paginator('list_objects')
            if recursive:
                paginator_result = paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={'PageSize': 1000})
            else:
                paginator_result = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/',
                                                      PaginationConfig={'PageSize': 1000})
            async for item in paginator_result.search('CommonPrefixes'):
                if item:
                    path = bucket + '/' + item.get('Prefix')
                    name = path.split('/')[-2]
                    if name:
                        result.append(FSEntry(name, path, 'dir'))
        return result

    async def upload_fileobj(
        self,
        fileobj: IO[Any],
        dst_path: str
    ) -> None:
        """Upload file object

        Parameters
        ----------
        fileobj : IO[Any]
            File object to upload.
        dst_path : str
            Destination path.
        """
        bucket, key = self._split_path(dst_path)
        await self.client.upload_fileobj(fileobj, bucket, key)

    @staticmethod
    def _split_path(path: str) -> list[str]:
        path = path.split('://')[-1]
        return path.split('/', maxsplit=1)
