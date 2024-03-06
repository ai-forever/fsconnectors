import yaml
import aioboto3
import aiofiles.tempfile
from contextlib import asynccontextmanager
from typing import Union, List, IO, Literal

from fsconnectors.utils.entry import FSEntry
from fsconnectors.utils.multipart import AsyncMultipartWriter
from fsconnectors.asyncio.connector import AsyncConnector


class AsyncS3Connector(AsyncConnector):
    """Async S3 connector."""

    @classmethod
    @asynccontextmanager
    async def connect(
        cls,
        endpoint_url: str,
        aws_access_key_id: str,
        aws_secret_access_key: str
    ) -> 'AsyncS3Connector':
        """Connects to file system.

        Parameters
        ----------
        endpoint_url : str
            Endpoint URL.
        aws_access_key_id : str
            AWS access key ID
        aws_secret_access_key : str
            AWS secret access key

        Yields
        -------
        AsyncS3Connector
            Class instance
        """
        self = cls()
        async with aioboto3.Session().client(
                's3', endpoint_url=endpoint_url,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
        ) as client:
            self.client = client
            yield self

    @classmethod
    @asynccontextmanager
    async def from_yaml(cls, path: str) -> 'AsyncS3Connector':
        """Creates class instance from configuration path.

        Parameters
        ----------
        path : str
            path to configuration file.

        Yields
        -------
        AsyncS3Connector
            Class instance
        """
        with open(path) as f:
            config = yaml.safe_load(f)
        async with cls.connect(**config) as self:
            yield self

    @asynccontextmanager
    async def open(self, path: str, mode: Literal['rb', 'wb'] = 'rb', multipart: bool = False) -> Union[IO, AsyncMultipartWriter]:
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
        IO
            File-like object.
        """
        bucket, key = self._split_path(path)
        if mode == 'rb':
            obj = await self.client.get_object(Bucket=bucket, Key=key)
            stream = obj['Body']
            yield stream
            stream.close()
        elif mode == 'wb':
            if multipart:
                stream = AsyncMultipartWriter.open(self.client, Bucket=bucket, Key=key)
                yield stream
                await stream.close()
            else:
                async with aiofiles.tempfile.TemporaryFile() as stream:
                    yield stream
                    await stream.seek(0)
                    await self.client.put_object(Body=await stream.read(), Bucket=bucket, Key=key)
        else:
            raise ValueError(f"invalid mode: '{mode}'")

    async def mkdir(self, path: str):
        path = path.rstrip('/') + '/'
        bucket, key = self._split_path(path)
        await self.client.put_object(Bucket=bucket, Key=key)

    async def copy(self, src_path: str, dst_path: str, recursive: bool = False):
        if recursive:
            src_path = src_path.rstrip('/') + '/'
            dst_path = dst_path.rstrip('/') + '/'
            src_bucket, src_key = self._split_path(src_path)
            dst_bucket, dst_key = self._split_path(dst_path)
            paths = await self.listdir(src_path, recursive)
            for path in paths:
                path_bucket, path_key = self._split_path(path)
                await self.client.copy(dict(Bucket=path_bucket, Key=path_key), dst_bucket, path_key.replace(src_key, dst_key))
        else:
            src_bucket, src_key = self._split_path(src_path)
            dst_bucket, dst_key = self._split_path(dst_path)
            await self.client.copy(dict(Bucket=src_bucket, Key=src_key), dst_bucket, dst_key)

    async def move(self, src_path: str, dst_path: str, recursive: bool = False):
        await self.copy(src_path, dst_path, recursive)
        await self.remove(src_path, recursive)

    async def remove(self, path: str, recursive: bool = False):
        if recursive:
            path = path.rstrip('/') + '/'
            paths = await self.listdir(path, recursive)
            for path in paths:
                path_bucket, path_key = self._split_path(path)
                await self.client.delete_object(Bucket=path_bucket, Key=path_key)
        else:
            bucket, key = self._split_path(path)
            await self.client.delete_object(Bucket=bucket, Key=key)

    async def listdir(self, path: str, recursive: bool = False) -> List[str]:
        entries = await self.scandir(path, recursive)
        if recursive:
            result = [entry.path for entry in entries]
        else:
            result = [entry.name for entry in entries]
        return result

    async def scandir(self, path: str, recursive: bool = False) -> List[FSEntry]:
        result = []
        path = path.rstrip('/') + '/'
        bucket, prefix = self._split_path(path)
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
        async for item in paginator_result.search('Contents'):
            if item:
                path = bucket + '/' + item.get('Key')
                name = path.split('/')[-1]
                if name:
                    size = item.get('Size')
                    last_modified = item.get('LastModified')
                    result.append(FSEntry(name, path, 'file', size, last_modified))
        return result

    @staticmethod
    def _split_path(path: str) -> List[str]:
        path = path.split('://')[-1]
        return path.split('/', maxsplit=1)
