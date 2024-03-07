from typing import Optional, Any, Dict


class MultipartWriter:
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
        self._part_info: Dict[Any, Any] = {'Parts': []}

    @classmethod
    def open(
        cls,
        client: Any,
        Bucket: str,
        Key: str
    ) -> 'MultipartWriter':
        self = cls(client, Bucket, Key)
        resp = self.client.create_multipart_upload(Bucket=self.bucket, Key=self.key)
        self._upload_id = resp['UploadId']
        return self

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

    def close(self) -> None:
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


class AsyncMultipartWriter:
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
        self._part_num = 0
        self._part_info: Dict[Any, Any] = {'Parts': []}

    @classmethod
    async def open(
        cls,
        client: Any,
        Bucket: str,
        Key: str
    ) -> 'AsyncMultipartWriter':
        self = cls(client, Bucket, Key)
        resp = await self.client.create_multipart_upload(Bucket=self.bucket, Key=self.key)
        self._upload_id = resp['UploadId']
        return self

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

    async def close(self) -> None:
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
