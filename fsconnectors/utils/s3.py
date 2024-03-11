import tempfile
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import Any, Optional

import asynctempfile


class MultipartWriter(AbstractContextManager[Any]):
    """Multipart S3 writer.

    Attributes
    ----------
    client : Any
        Boto3 S3 client.
    bucket : str
        S3 bucket.
    key : str
        S3 file key.
    """

    def __init__(
        self,
        client: Any,
        bucket: str,
        key: str
    ):
        self.client = client
        self.bucket = bucket
        self.key = key
        self._upload_id = ''
        self._part_num = 0
        self._part_info: dict[Any, Any] = {'Parts': []}

    def __enter__(self) -> 'MultipartWriter':
        resp = self.client.create_multipart_upload(Bucket=self.bucket, Key=self.key)
        self._upload_id = resp['UploadId']
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        resp = self.client.list_parts(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self._upload_id
        )
        uploaded_parts = resp['Parts']
        if len(uploaded_parts) == self._part_num:
            parts_sorted = sorted(self._part_info['Parts'], key=lambda x: x['PartNumber'])
            self._part_info['Parts'] = parts_sorted
            self.client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=self.key,
                UploadId=self._upload_id,
                MultipartUpload=self._part_info
            )
        else:
            loss = int((self._part_num - len(uploaded_parts)) / self._part_num * 100)
            self.client.abort_multipart_upload(
                Bucket=self.bucket,
                Key=self.key,
                UploadId=self._upload_id
            )
            raise RuntimeError(f'Write aborted!\n'
                               f'{self._part_num} parts transmitted, {len(uploaded_parts)} received, {loss}% loss')

    def write(self, data: bytes, part_num: Optional[int] = None) -> None:
        if part_num is None:
            part_num = self._part_num = self._part_num + 1
        elif 1 <= part_num <= 10000:
            self._part_num += 1
        else:
            raise ValueError('part_num must be an integer between 1 and 1000')
        resp = self.client.upload_part(Bucket=self.bucket, Body=data,
                                       UploadId=self._upload_id, PartNumber=part_num, Key=self.key)
        self._part_info['Parts'].append(
            {
                'PartNumber': part_num,
                'ETag': resp['ETag']
            }
        )


class AsyncMultipartWriter(AbstractAsyncContextManager[Any]):
    """Async multipart S3 writer.

    Attributes
    ----------
    client : Any
        Aioboto3 S3 client.
    bucket : str
        S3 bucket.
    key : str
        S3 file key.
    """

    def __init__(
        self,
        client: Any,
        bucket: str,
        key: str
    ):
        self.client = client
        self.bucket = bucket
        self.key = key
        self._upload_id = ''
        self._part_num: int = 0
        self._part_info: dict[Any, Any] = {'Parts': []}

    async def __aenter__(self) -> 'AsyncMultipartWriter':
        resp = await self.client.create_multipart_upload(Bucket=self.bucket, Key=self.key)
        self._upload_id = resp['UploadId']
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        resp = await self.client.list_parts(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self._upload_id
        )
        uploaded_parts = resp['Parts']
        if len(uploaded_parts) == self._part_num:
            parts_sorted = sorted(self._part_info['Parts'], key=lambda x: x['PartNumber'])
            self._part_info['Parts'] = parts_sorted
            await self.client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=self.key,
                UploadId=self._upload_id,
                MultipartUpload=self._part_info
            )
        else:
            loss = int((self._part_num - len(uploaded_parts)) / self._part_num * 100)
            await self.client.abort_multipart_upload(
                Bucket=self.bucket,
                Key=self.key,
                UploadId=self._upload_id
            )
            raise RuntimeError(f'Write aborted!\n'
                               f'{self._part_num} parts transmitted, {len(uploaded_parts)} received, {loss}% loss')

    async def write(self, data: bytes, part_num: Optional[int] = None) -> None:
        if part_num is None:
            part_num = self._part_num = self._part_num + 1
        elif 1 <= part_num <= 10000:
            self._part_num += 1
        else:
            raise ValueError('part_num must be an integer between 1 and 1000')
        resp = await self.client.upload_part(Bucket=self.bucket, Body=data,
                                             UploadId=self._upload_id, PartNumber=part_num, Key=self.key)
        self._part_info['Parts'].append(
            {
                'PartNumber': part_num,
                'ETag': resp['ETag']
            }
        )


class SinglepartWriter(AbstractContextManager[Any]):
    """Singlepart S3 writer.

    Attributes
    ----------
    client : Any
        Boto3 S3 client.
    bucket : str
        S3 bucket.
    key : str
        S3 file key.
    """

    def __init__(
        self,
        client: Any,
        bucket: str,
        key: str
    ):
        self.client = client
        self.bucket = bucket
        self.key = key

    def __enter__(self) -> 'SinglepartWriter':
        self.file = tempfile.NamedTemporaryFile()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.file.seek(0)
        self.client.put_object(Body=self.file.read(), Bucket=self.bucket, Key=self.key)

    def write(self, data: bytes) -> None:
        self.file.write(data)


class AsyncSinglepartWriter(AbstractAsyncContextManager[Any]):
    """Async singlepart S3 writer.

    Attributes
    ----------
    client : Any
        Boto3 S3 client.
    bucket : str
        S3 bucket.
    key : str
        S3 file key.
    """

    def __init__(
        self,
        client: Any,
        bucket: str,
        key: str
    ):
        self.client = client
        self.bucket = bucket
        self.key = key

    async def __aenter__(self) -> 'AsyncSinglepartWriter':
        self.file = await asynctempfile.TemporaryFile()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.file.seek(0)
        await self.client.put_object(Body=await self.file.read(), Bucket=self.bucket, Key=self.key)

    async def write(self, data: bytes) -> None:
        await self.file.write(data)


class S3Reader(AbstractContextManager[Any]):
    """S3 stream reader.

    Attributes
    ----------
    client : Any
        Boto3 S3 client.
    bucket : str
        S3 bucket.
    key : str
        S3 file key.
    """

    def __init__(
        self,
        client: Any,
        bucket: str,
        key: str
    ):
        self.client = client
        self.bucket = bucket
        self.key = key

    def __enter__(self) -> 'S3Reader':
        obj = self.client.get_object(Bucket=self.bucket, Key=self.key)
        self.stream = obj['Body']
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.stream.close()

    def read(self, chunk: Optional[int] = None) -> Any:
        return self.stream.read(chunk)


class AsyncS3Reader(AbstractAsyncContextManager[Any]):
    """Async S3 stream reader.

    Attributes
    ----------
    client : Any
        Boto3 S3 client.
    bucket : str
        S3 bucket.
    key : str
        S3 file key.
    """

    def __init__(
        self,
        client: Any,
        bucket: str,
        key: str
    ):
        self.client = client
        self.bucket = bucket
        self.key = key

    async def __aenter__(self) -> 'AsyncS3Reader':
        obj = await self.client.get_object(Bucket=self.bucket, Key=self.key)
        self.stream = obj['Body']
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.stream.close()

    async def read(self, chunk: Optional[int] = None) -> Any:
        return await self.stream.read(chunk)
