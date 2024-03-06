import yaml
import boto3
import tempfile
from contextlib import contextmanager
from typing import Union, List, IO, Literal

from fsconnectors.connector import Connector
from fsconnectors.utils.entry import FSEntry
from fsconnectors.utils.s3 import MultipartWriter


class S3Connector(Connector):
    """S3 connector"""

    def __init__(
        self,
        endpoint_url: str,
        aws_access_key_id: str,
        aws_secret_access_key: str
    ):
        self.client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

    @classmethod
    def from_yaml(cls, path: str) -> 'S3Connector':
        with open(path) as f:
            config = yaml.safe_load(f)
        return cls(**config)

    @contextmanager
    def open(self, path: str, mode: Literal['rb', 'wb'] = 'rb', multipart: bool = False) -> Union[IO, MultipartWriter]:
        try:
            bucket, key = self._split_path(path)
            if mode == 'rb':
                obj = self.client.get_object(Bucket=bucket, Key=key)
                stream = obj['Body']
            elif mode == 'wb':
                if multipart:
                    stream = MultipartWriter.open(self.client, Bucket=bucket, Key=key)
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
                    self.client.put_object(Body=stream.read(), Bucket=bucket, Key=key)
                    stream.close()

    def mkdir(self, path: str):
        path = path.rstrip('/') + '/'
        bucket, key = self._split_path(path)
        self.client.put_object(Bucket=bucket, Key=key)

    def copy(self, src_path: str, dst_path: str, recursive: bool = False):
        if recursive:
            src_path = src_path.rstrip('/') + '/'
            dst_path = dst_path.rstrip('/') + '/'
            src_bucket, src_key = self._split_path(src_path)
            dst_bucket, dst_key = self._split_path(dst_path)
            paths = self.listdir(src_path, recursive)
            for path in paths:
                path_bucket, path_key = self._split_path(path)
                self.client.copy(dict(Bucket=path_bucket, Key=path_key), dst_bucket, path_key.replace(src_key, dst_key))
        else:
            src_bucket, src_key = self._split_path(src_path)
            dst_bucket, dst_key = self._split_path(dst_path)
            self.client.copy(dict(Bucket=src_bucket, Key=src_key), dst_bucket, dst_key)

    def move(self, src_path: str, dst_path: str, recursive: bool = False):
        self.copy(src_path, dst_path, recursive)
        self.remove(src_path, recursive)

    def remove(self, path: str, recursive: bool = False):
        if recursive:
            path = path.rstrip('/') + '/'
            paths = self.listdir(path, recursive)
            for path in paths:
                path_bucket, path_key = self._split_path(path)
                self.client.delete_object(Bucket=path_bucket, Key=path_key)
        else:
            bucket, key = self._split_path(path)
            self.client.delete_object(Bucket=bucket, Key=key)

    def listdir(self, path: str, recursive: bool = False) -> List[str]:
        entries = self.scandir(path, recursive)
        if recursive:
            result = [entry.path for entry in entries]
        else:
            result = [entry.name for entry in entries]
        return result

    def scandir(self, path: str, recursive: bool = False) -> List[FSEntry]:
        result = []
        path = path.rstrip('/') + '/'
        bucket, prefix = self._split_path(path)
        paginator = self.client.get_paginator('list_objects')
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
        return result

    @staticmethod
    def _split_path(path: str) -> List[str]:
        path = path.split('://')[-1]
        return path.split('/', maxsplit=1)
