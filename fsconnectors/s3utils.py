import os
import re
import math
import yaml
import asyncio
import argparse
import aiofiles
import platform
import aioboto3
import asyncio_pool
from tqdm.auto import tqdm
from typing import List, Tuple, Dict


def parse_config(path: str) -> Dict[str, str]:
    with open(path) as f:
        config = yaml.safe_load(f)
    for field in ['endpoint_url', 'aws_secret_access_key', 'aws_access_key_id']:
        assert field in config.keys(), f"Configuration file must contain '{field}' field"
    return config


class S3Util:

    def __init__(
        self,
        endpoint_url: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
    ):
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.s3_regex = re.compile('^s3://(.*?)/(.*)$')
        self.local_platform = platform.system()

    async def upload(
        self,
        local_path: str,
        s3_path: str,
        num_workers: int = 16,
        num_retries: int = 3,
        chunk_size: int = 1024 * 1024 * 16,
        large_file_size: int = 1024 * 1024 * 128,
        max_chunks_number: int = 999,
        timeout: int = 3
    ) -> List[str]:
        s3_path, local_path = self._prepare_paths(s3_path, local_path)
        path2size = self._scan_local_files(local_path)
        files_pbar = tqdm(total=len(path2size), desc='Files')
        bytes_pbar = tqdm(total=sum(path2size.values()), desc='Bytes')
        small_files, large_files = self._split_small_large_files(path2size, large_file_size)
        async with aioboto3.Session().client(
            's3', endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        ) as client:
            error_files = []
            error_files += await self._upload_small_files(
                client=client, path2size=small_files,
                local_path=local_path, s3_path=s3_path,
                num_workers=num_workers, num_retries=num_retries, timeout=timeout,
                files_pbar=files_pbar, bytes_pbar=bytes_pbar
            )
            error_files += await self._upload_large_files(
                client=client, path2size=large_files,
                local_path=local_path, s3_path=s3_path,
                num_workers=num_workers, num_retries=num_retries, timeout=timeout,
                chunk_size=chunk_size, max_chunks_number=max_chunks_number,
                files_pbar=files_pbar, bytes_pbar=bytes_pbar
            )
        return error_files

    async def download(
        self,
        s3_path: str,
        local_path: str,
        num_workers: int = 16,
        num_retries: int = 3,
        chunk_size: int = 1024 * 1024 * 16,
        timeout: int = 3
    ) -> List[str]:
        s3_path, local_path = self._prepare_paths(s3_path, local_path)
        async with aioboto3.Session().client(
            's3', endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        ) as client:
            path2size = await self._scan_s3_files(client, s3_path)
            files_pbar = tqdm(total=len(path2size), desc='Files')
            bytes_pbar = tqdm(total=sum(path2size.values()), desc='Bytes')
            error_files = []
            async with asyncio_pool.AioPool(size=num_workers) as pool:
                for source_path, file_size in path2size.items():
                    destination_path = source_path.replace(s3_path, local_path, 1)
                    status = await pool.spawn(self._download_file(
                        client, source_path, destination_path, file_size, files_pbar, bytes_pbar,
                        chunk_size, num_retries, timeout
                    ))
                    if not status:
                        error_files.append(source_path)
        return error_files

    def _prepare_paths(self, s3_path: str, local_path: str) -> Tuple[str, str]:
        if not s3_path.startswith('s3://'):
            s3_path = f's3://{s3_path}'
        if self.local_platform == 'Windows':
            local_path = re.sub(r'\\+', '/', local_path)
        s3_path = s3_path.rstrip('/')
        local_path = local_path.rstrip('/')
        return s3_path, local_path

    def _scan_local_files(self, local_path: str) -> Dict[str, int]:
        path2size = dict()
        if os.path.isfile(local_path):
            path2size[local_path] = os.path.getsize(local_path)
        elif os.path.isdir(local_path):
            for path, _, files in tqdm(os.walk(local_path), desc='Evaluating', leave=False):
                for file in files:
                    file_path = os.path.join(path, file)
                    if self.local_platform == 'Windows':
                        file_path = re.sub(r'\\+', '/', file_path)
                    path2size[file_path] = os.path.getsize(file_path)
            assert len(path2size) > 0, f"No files found in directory '{local_path}'"
        else:
            raise FileNotFoundError(f"No such file or directory: '{local_path}'")
        return path2size

    async def _scan_s3_files(
        self,
        client,
        s3_path: str
    ) -> Dict[str, int]:
        path2size = dict()
        pbar = tqdm(desc='Evaluating', leave=False)
        paginator = client.get_paginator('list_objects_v2')
        _, bucket, prefix, _ = self.s3_regex.split(s3_path)
        async for result in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for file in result.get('Contents'):
                file_path = file.get('Key')
                file_size = file.get('Size')
                path2size[f's3://{bucket}/{file_path}'] = file_size
            pbar.update(1)
        if len(path2size) == 0:
            raise FileNotFoundError(f"No such file or directory: '{s3_path}'")
        return path2size

    @staticmethod
    def _split_small_large_files(
        path2size: Dict[str, int],
        large_file_size: int
    ) -> Tuple[Dict[str, int], Dict[str, int]]:
        small_files, large_files = dict(), dict()
        for file_path, file_size in path2size.items():
            if file_size > large_file_size:
                large_files[file_path] = file_size
            else:
                small_files[file_path] = file_size
        return small_files, large_files

    async def _upload_small_files(
        self,
        client,
        path2size: Dict[str, int],
        local_path: str,
        s3_path: str,
        files_pbar: tqdm,
        bytes_pbar: tqdm,
        num_workers: int = 16,
        num_retries: int = 3,
        timeout: int = 3
    ) -> List[str]:
        error_files = []
        async with asyncio_pool.AioPool(size=num_workers) as pool:
            for source_path, file_size in path2size.items():
                destination_path = source_path.replace(local_path, s3_path, 1)
                status = await pool.spawn(self._upload_small_file(
                    client, source_path, destination_path, file_size, files_pbar, bytes_pbar, num_retries, timeout
                ))
                if not status:
                    error_files.append(source_path)
        return error_files

    async def _upload_small_file(
        self,
        client,
        source_path: str,
        destination_path: str,
        file_size: int,
        files_pbar: tqdm,
        bytes_pbar: tqdm,
        num_retries: int = 3,
        timeout: int = 3
    ) -> bool:
        retries = 0
        while retries < num_retries:
            try:
                retries += 1
                _, bucket, key, _ = self.s3_regex.split(destination_path)
                async with aiofiles.open(source_path, 'rb') as file:
                    await client.upload_fileobj(file, Bucket=bucket, Key=key)
                files_pbar.update(1)
                bytes_pbar.update(file_size)
                return True
            except Exception as err:
                err_msg = err
                await asyncio.sleep(timeout)
        print(err_msg)
        files_pbar.update(1)
        bytes_pbar.update(file_size)
        return False

    async def _upload_large_files(
        self,
        client,
        path2size: Dict[str, int],
        local_path: str,
        s3_path: str,
        files_pbar: tqdm,
        bytes_pbar: tqdm,
        num_workers: int = 16,
        num_retries: int = 3,
        chunk_size: int = 1024 * 1024 * 16,
        max_chunks_number: int = 999,
        timeout: int = 3
    ) -> List[str]:
        error_files = []
        for source_path, file_size in path2size.items():
            destination_path = source_path.replace(local_path, s3_path, 1)
            status = await self._upload_large_file(
                client, source_path, destination_path, file_size, files_pbar, bytes_pbar,
                num_workers, num_retries, chunk_size, max_chunks_number, timeout
            )
            if not status:
                error_files.append(source_path)
        return error_files

    async def _upload_large_file(
        self,
        client,
        source_path: str,
        destination_path: str,
        file_size: int,
        files_pbar: tqdm,
        bytes_pbar: tqdm,
        num_workers: int = 16,
        num_retries: int = 3,
        chunk_size: int = 1024 * 1024 * 16,
        max_chunks_number: int = 999,
        timeout: int = 3
    ) -> bool:
        chunks_number = int(math.ceil(file_size / float(chunk_size)))
        if chunks_number > max_chunks_number:
            chunk_size = file_size // max_chunks_number + 1
            chunks_number = int(math.ceil(file_size / float(chunk_size)))
        _, bucket, key, _ = self.s3_regex.split(destination_path)
        resp = await client.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = resp['UploadId']
        part_info = {
            'Parts': []
        }
        async with aiofiles.open(source_path, 'rb') as file:
            async with asyncio_pool.AioPool(size=num_workers) as pool:
                for part_number in range(1, chunks_number + 1):
                    await file.seek((part_number - 1) * chunk_size)
                    chunk = await file.read(chunk_size)
                    await pool.spawn(self._upload_chunk(
                        client, chunk, bucket, key, upload_id, part_number, part_info,
                        bytes_pbar, num_retries, timeout
                    ))
        resp = await client.list_parts(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id
        )
        uploaded_parts = resp['Parts']
        if len(uploaded_parts) == chunks_number:
            parts_sorted = sorted(part_info['Parts'], key=lambda x: x['PartNumber'])
            part_info['Parts'] = parts_sorted
            await client.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload=part_info
            )
            files_pbar.update(1)
            return True
        else:
            await client.abort_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id
            )
            files_pbar.update(1)
            return False

    @staticmethod
    async def _upload_chunk(
        client,
        chunk: bytes,
        bucket: str,
        key: str,
        upload_id: str,
        part_number: int,
        part_info: dict,
        bytes_pbar: tqdm,
        num_retries: int = 3,
        timeout: int = 3
    ):
        retries = 0
        while retries < num_retries:
            try:
                retries += 1
                resp = await client.upload_part(
                    Bucket=bucket,
                    Body=chunk,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Key=key
                )
                part_info['Parts'].append(
                    {
                        'PartNumber': part_number,
                        'ETag': resp['ETag']
                    }
                )
                bytes_pbar.update(len(chunk))
                return
            except Exception as err:
                err_msg = err
                await asyncio.sleep(timeout)
        print(err_msg)
        bytes_pbar.update(len(chunk))

    async def _download_file(
        self,
        client,
        source_path: str,
        destination_path: str,
        file_size: int,
        files_pbar: tqdm,
        bytes_pbar: tqdm,
        chunk_size: int = 1024 * 1024 * 16,
        num_retries: int = 3,
        timeout: int = 3
    ) -> bool:
        retries = 0
        while retries < num_retries:
            try:
                retries += 1
                _, bucket, key, _ = self.s3_regex.split(source_path)
                os.makedirs(os.path.dirname(destination_path), exist_ok=True)
                s3_obj = await client.get_object(Bucket=bucket, Key=key)
                obj_info = s3_obj["ResponseMetadata"]["HTTPHeaders"]
                stream = s3_obj["Body"]
                async with aiofiles.open(destination_path, 'wb') as file:
                    chunk = await stream.read(chunk_size)
                    while chunk:
                        await file.write(chunk)
                        bytes_pbar.update(len(chunk))
                        chunk = await stream.read(chunk_size)
                files_pbar.update(1)
                return True
            except Exception as err:
                err_msg = err
                await asyncio.sleep(timeout)
        print(err_msg)
        files_pbar.update(1)
        bytes_pbar.update(file_size)
        return False


async def main():
    parser = argparse.ArgumentParser(
        prog='fsconnectors',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='to see action help message:\n  fsconnectors upload -h\n  fsconnectors download -h'
    )
    subparsers = parser.add_subparsers(dest='action')
    upload_parser = subparsers.add_parser('upload', help='upload to S3')
    download_parser = subparsers.add_parser('download', help='download from S3')
    subparsers.required = True
    for subparser in [upload_parser, download_parser]:
        subparser.add_argument('--s3_path', required=True, type=str, help='S3 path to folder or file')
        subparser.add_argument('--local_path', required=True, type=str, help='local path to folder or file')
        subparser.add_argument('--config_path', required=True, type=str, help='path to configuration file')
        subparser.add_argument('--workers', type=int, default=16, help='Number of parallel async workers')
    args = parser.parse_args()

    config = parse_config(args.config_path)

    s3util = S3Util(**config)
    if args.action == 'upload':
        error_files = await s3util.upload(local_path=args.local_path, s3_path=args.s3_path, num_workers=args.workers)
        print(f'Error files: {error_files}')
    elif args.action == 'download':
        error_files = await s3util.download(local_path=args.local_path, s3_path=args.s3_path, num_workers=args.workers)
        print(f'Error files: {error_files}')
    else:
        raise ValueError(f"Action mast be 'upload' or 'download', got '{args.action}'")


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
