import tempfile
from contextlib import contextmanager
from typing import Any, List

import boto3
import yaml

from fsconnectors.connector import Connector
from fsconnectors.utils.entry import FSEntry
from fsconnectors.utils.multipart import MultipartWriter


class S3Connector(Connector):
    """S3 connector.

    Attributes
    ----------
    endpoint_url : str
        Endpoint URL.
    aws_access_key_id : str
        AWS access key ID.
    aws_secret_access_key : str
        AWS secret access key.
    """

    def __init__(
        self,
        endpoint_url: str,
        aws_access_key_id: str,
        aws_secret_access_key: str
    ):
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    @classmethod
    def from_yaml(cls, path: str) -> 'S3Connector':
        """Creates class instance from configuration path.

        Parameters
        ----------
        path : str
            path to configuration file.

        Returns
        -------
        S3Connector
            Class instance.
        """
        with open(path) as f:
            config = yaml.safe_load(f)
        return cls(**config)

    @contextmanager
    def open(self, path: str, mode: str = 'rb', multipart: bool = False) -> Any:
        """Open file.

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
        Any
            Readable/writable file-like object.
        """
        try:
            client = self._get_client()
            bucket, key = self._split_path(path)
            if mode == 'rb':
                obj = client.get_object(Bucket=bucket, Key=key)
                stream = obj['Body']
            elif mode == 'wb':
                if multipart:
                    stream = MultipartWriter.open(client, Bucket=bucket, Key=key)
                else:
                    stream = tempfile.TemporaryFile()
            else:
                raise ValueError(f"invalid mode: '{mode}'")
            yield stream
        finally:
            if mode == 'rb':
                stream.close()
            elif mode == 'wb':
                if multipart:
                    stream.close()
                else:
                    stream.seek(0)
                    client.put_object(Body=stream.read(), Bucket=bucket, Key=key)
                    stream.close()
            client.close()

    def mkdir(self, path: str) -> None:
        client = self._get_client()
        path = path.rstrip('/') + '/'
        bucket, key = self._split_path(path)
        client.put_object(Bucket=bucket, Key=key)
        client.close()

    def copy(self, src_path: str, dst_path: str, recursive: bool = False) -> None:
        client = self._get_client()
        if recursive:
            src_path = src_path.rstrip('/') + '/'
            dst_path = dst_path.rstrip('/') + '/'
            src_bucket, src_key = self._split_path(src_path)
            dst_bucket, dst_key = self._split_path(dst_path)
            paths = self.listdir(src_path, recursive)
            for path in paths:
                path_bucket, path_key = self._split_path(path)
                client.copy({'Bucket': path_bucket, 'Key': path_key}, dst_bucket, path_key.replace(src_key, dst_key))
        else:
            src_bucket, src_key = self._split_path(src_path)
            dst_bucket, dst_key = self._split_path(dst_path)
            client.copy({'Bucket': src_bucket, 'Key': src_key}, dst_bucket, dst_key)
        client.close()

    def move(self, src_path: str, dst_path: str, recursive: bool = False) -> None:
        self.copy(src_path, dst_path, recursive)
        self.remove(src_path, recursive)

    def remove(self, path: str, recursive: bool = False) -> None:
        client = self._get_client()
        if recursive:
            path = path.rstrip('/') + '/'
            paths = self.listdir(path, recursive)
            for path in paths:
                path_bucket, path_key = self._split_path(path)
                client.delete_object(Bucket=path_bucket, Key=path_key)
        else:
            bucket, key = self._split_path(path)
            client.delete_object(Bucket=bucket, Key=key)
        client.close()

    def listdir(self, path: str, recursive: bool = False) -> List[str]:
        entries = self.scandir(path, recursive)
        if recursive:
            result = [entry.path for entry in entries]
        else:
            result = [entry.name for entry in entries]
        return result

    def scandir(self, path: str, recursive: bool = False) -> List[FSEntry]:
        client = self._get_client()
        result = []
        path = path.rstrip('/') + '/'
        bucket, prefix = self._split_path(path)
        paginator = client.get_paginator('list_objects')
        if recursive:
            paginator_result = paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={'PageSize': 1000})
        else:
            paginator_result = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/',
                                                  PaginationConfig={'PageSize': 1000})
        for item in paginator_result.search('CommonPrefixes'):
            if item:
                path = bucket + '/' + item.get('Prefix')
                name = path.split('/')[-2]
                if name:
                    result.append(FSEntry(name, path, 'dir'))
        for item in paginator_result.search('Contents'):
            if item:
                path = bucket + '/' + item.get('Key')
                name = path.split('/')[-1]
                if name:
                    size = item.get('Size')
                    last_modified = item.get('LastModified')
                    result.append(FSEntry(name, path, 'file', size, last_modified))
        client.close()
        return result

    def _get_client(self) -> Any:
        client = boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )
        return client

    @staticmethod
    def _split_path(path: str) -> List[str]:
        path = path.split('://')[-1]
        return path.split('/', maxsplit=1)
