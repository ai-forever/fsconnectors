import argparse
import asyncio
import math
import os.path
import platform
import re
from typing import Union

import asyncio_pool
from tqdm.auto import tqdm

from fsconnectors import AsyncLocalConnector, AsyncS3Connector
from fsconnectors.asyncio.connector import AsyncConnector
from fsconnectors.utils.entry import FSEntry
from fsconnectors.utils.s3 import (
    AsyncMultipartWriter,
    AsyncS3Reader,
    AsyncSinglepartWriter,
)


class CLI:
    """Async S3 upload/download class.

    Attributes
    ----------
    s3_connector : AsyncConnector
        Async S3 connector.
    local_connector : AsyncConnector
        Async local connector.
    """

    def __init__(
        self,
        s3_connector: AsyncConnector,
        local_connector: AsyncConnector
    ):
        self.s3_connector = s3_connector
        self.local_connector = local_connector

    async def upload(
        self,
        local_path: str,
        s3_path: str,
        num_workers: int = 16,
        multipart: bool = False,
        num_retries: int = 3,
        chunk_size: int = 1024 * 1024 * 16,
        large_file_size: int = 1024 * 1024 * 128,
        max_chunks_number: int = 999,
        delay: int = 3
    ) -> list[str]:
        """Upload to S3.

        Parameters
        ----------
        local_path : str
            Local directory path.
        s3_path : str
            S3 directory path.
        num_workers : int, default=16
            Max workers.
        multipart : bool, default=False
            Use multipart upload.
        num_retries : int, default=3
            Max retries per file.
        chunk_size : int, default=1024 * 1024 * 16
            Chunk size in bytes for multipart upload if enabled.
        large_file_size : int, default=1024 * 1024 * 128
            Min file size for multipart upload if enabled.
        max_chunks_number : int, default=999
            Max chunks for multipart upload if enabled.
        delay : int, default=3
            Retry delay.

        Returns
        -------
        list[str]
            Error files.
        """
        s3_path, local_path = self._prepare_paths(s3_path, local_path)
        async with self.local_connector.connect() as lc:
            async with self.s3_connector.connect() as sc:
                files = await lc.scandir(local_path, recursive=True)
                files_pbar = tqdm(total=len([file for file in files if file.type == 'file']), desc='Files')
                bytes_pbar = tqdm(total=sum([file.size if file.size is not None else 0 for file in files]), desc='Bytes')
                error_files = []
                if multipart:
                    small_files, large_files = self._split_small_large_files(files, large_file_size)
                    error_files += await self._upload_files(
                        lc, sc, small_files, local_path, s3_path,
                        files_pbar, bytes_pbar, num_workers, num_retries, delay
                    )
                    error_files += await self._upload_files_multipart(
                        lc, sc, large_files, local_path, s3_path, files_pbar, bytes_pbar,
                        num_workers, num_retries, chunk_size, max_chunks_number, delay
                    )
                else:
                    error_files += await self._upload_files(
                        lc, sc, files, local_path, s3_path,
                        files_pbar, bytes_pbar, num_workers, num_retries, delay
                    )
        return error_files

    async def download(
        self,
        local_path: str,
        s3_path: str,
        num_workers: int = 16,
        num_retries: int = 3,
        chunk_size: int = 1024 * 1024 * 16,
        delay: int = 3
    ) -> list[str]:
        """Download from S3.

        Parameters
        ----------
        local_path : str
            Local directory path.
        s3_path : str
            S3 directory path.
        num_workers : int, default=16
            Max workers.
        num_retries : int, default=3
            Max retries per file.
        chunk_size : int, default=1024 * 1024 * 16
            Chunk size in bytes.
        delay : int, default=3
            Retry delay.

        Returns
        -------
        list[str]
            Error files.
        """
        s3_path, local_path = self._prepare_paths(s3_path, local_path)
        print(s3_path, local_path)
        async with self.local_connector.connect() as lc:
            async with self.s3_connector.connect() as sc:
                files = await sc.scandir(s3_path, recursive=True)
                files_pbar = tqdm(total=len([file for file in files if file.type == 'file']), desc='Files')
                bytes_pbar = tqdm(total=sum([file.size if file.size is not None else 0 for file in files]), desc='Bytes')
                error_files = []
                async with asyncio_pool.AioPool(size=num_workers) as pool:
                    for file in files:
                        if file.size is not None:
                            destination_path = file.path.replace(s3_path, local_path, 1)
                            status = await pool.spawn(self._download_file(
                                lc, sc, file.path, destination_path, file.size,
                                files_pbar, bytes_pbar, chunk_size, num_retries, delay
                            ))
                            if not status:
                                error_files.append(file.path)
        return error_files

    async def _upload_files(
        self,
        local_connection: AsyncConnector,
        s3_connection: AsyncConnector,
        files: list[FSEntry],
        local_path: str,
        s3_path: str,
        files_pbar: tqdm,
        bytes_pbar: tqdm,
        num_workers: int = 16,
        num_retries: int = 3,
        delay: int = 3
    ) -> list[str]:
        error_files = []
        async with asyncio_pool.AioPool(size=num_workers) as pool:
            for file in files:
                if file.size is not None:
                    destination_path = file.path.replace(local_path, s3_path, 1)
                    status = await pool.spawn(self._upload_file(
                        local_connection, s3_connection, file.path, destination_path, file.size,
                        files_pbar, bytes_pbar, num_retries, delay
                    ))
                    if not status:
                        error_files.append(file.path)
        return error_files

    @staticmethod
    async def _upload_file(
        local_connection: AsyncConnector,
        s3_connection: AsyncConnector,
        source_path: str,
        destination_path: str,
        file_size: int,
        files_pbar: tqdm,
        bytes_pbar: tqdm,
        num_retries: int = 3,
        delay: int = 3
    ) -> bool:
        retries = 0
        while retries < num_retries:
            try:
                retries += 1
                async with local_connection.open(source_path, 'rb') as src_file:
                    if isinstance(s3_connection, AsyncS3Connector):
                        await s3_connection.upload_fileobj(src_file, destination_path)
                files_pbar.update(1)
                bytes_pbar.update(file_size)
                return True
            except Exception as err:
                err_msg = err
                await asyncio.sleep(delay)
        print(err_msg)
        files_pbar.update(1)
        bytes_pbar.update(file_size)
        return False

    async def _upload_files_multipart(
        self,
        local_connection: AsyncConnector,
        s3_connection: AsyncConnector,
        files: list[FSEntry],
        local_path: str,
        s3_path: str,
        files_pbar: tqdm,
        bytes_pbar: tqdm,
        num_workers: int = 16,
        num_retries: int = 3,
        chunk_size: int = 1024 * 1024 * 16,
        max_chunks_number: int = 999,
        delay: int = 3
    ) -> list[str]:
        error_files = []
        for file in files:
            if file.size is not None:
                destination_path = file.path.replace(local_path, s3_path, 1)
                status = await self._upload_file_multipart(
                    local_connection, s3_connection, file.path, destination_path, file.size,
                    files_pbar, bytes_pbar, num_workers, num_retries, chunk_size, max_chunks_number, delay
                )
                if not status:
                    error_files.append(file.path)
        return error_files

    async def _upload_file_multipart(
        self,
        local_connection: AsyncConnector,
        s3_connection: AsyncConnector,
        source_path: str,
        destination_path: str,
        file_size: int,
        files_pbar: tqdm,
        bytes_pbar: tqdm,
        num_workers: int = 16,
        num_retries: int = 3,
        chunk_size: int = 1024 * 1024 * 16,
        max_chunks_number: int = 999,
        delay: int = 3
    ) -> bool:
        chunks_number = int(math.ceil(file_size / float(chunk_size)))
        if chunks_number > max_chunks_number:
            chunk_size = file_size // max_chunks_number + 1
            chunks_number = int(math.ceil(file_size / float(chunk_size)))
        retries = 0
        while retries < num_retries:
            try:
                retries += 1
                async with local_connection.open(source_path, 'rb') as src_file:
                    if isinstance(s3_connection, AsyncS3Connector):
                        async with s3_connection.open(destination_path, 'wb', multipart=True) as dst_file:
                            async with asyncio_pool.AioPool(size=num_workers) as pool:
                                for part_number in range(1, chunks_number + 1):
                                    await src_file.seek((part_number - 1) * chunk_size)
                                    chunk = await src_file.read(chunk_size)
                                    await pool.spawn(self._upload_chunk(dst_file, chunk, part_number, bytes_pbar))
                files_pbar.update(1)
                return True
            except Exception as err:
                err_msg = err
                await asyncio.sleep(delay)
        print(err_msg)
        files_pbar.update(1)
        bytes_pbar.update(file_size)
        return False

    @staticmethod
    async def _upload_chunk(
        dst_file: Union[AsyncS3Reader, AsyncMultipartWriter, AsyncSinglepartWriter],
        chunk: bytes,
        part_number: int,
        bytes_pbar: tqdm
    ) -> None:
        if isinstance(dst_file, AsyncMultipartWriter):
            await dst_file.write(chunk, part_num=part_number)
            bytes_pbar.update(len(chunk))

    @staticmethod
    async def _download_file(
        local_connection: AsyncConnector,
        s3_connection: AsyncConnector,
        source_path: str,
        destination_path: str,
        file_size: int,
        files_pbar: tqdm,
        bytes_pbar: tqdm,
        chunk_size: int = 1024 * 1024 * 16,
        num_retries: int = 3,
        delay: int = 3
    ) -> bool:
        retries = 0
        await local_connection.mkdir(os.path.dirname(destination_path))
        while retries < num_retries:
            try:
                retries += 1
                async with s3_connection.open(source_path, 'rb') as src_file:
                    async with local_connection.open(destination_path, 'wb') as dst_file:
                        chunk = await src_file.read(chunk_size)
                        while chunk:
                            await dst_file.write(chunk)
                            bytes_pbar.update(len(chunk))
                            chunk = await src_file.read(chunk_size)
                files_pbar.update(1)
                return True
            except Exception as err:
                err_msg = err
                await asyncio.sleep(delay)
        print(err_msg)
        files_pbar.update(1)
        bytes_pbar.update(file_size)
        return False

    @staticmethod
    def _prepare_paths(s3_path: str, local_path: str) -> tuple[str, str]:
        if platform.system() == 'Windows':
            local_path = re.sub(r'\\+', '/', local_path)
        s3_path = s3_path.split('://')[-1].rstrip('/')
        local_path = local_path.rstrip('/')
        return s3_path, local_path

    @staticmethod
    def _split_small_large_files(
        files: list[FSEntry],
        large_file_size: int
    ) -> tuple[list[FSEntry], list[FSEntry]]:
        small_files, large_files = [], []
        for file in files:
            if file.size is not None and file.size > large_file_size:
                large_files.append(file)
            else:
                small_files.append(file)
        return small_files, large_files


async def main() -> None:
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
        subparser.add_argument('--s3_path', required=True, type=str, help='S3 folder path')
        subparser.add_argument('--local_path', required=True, type=str, help='local folder path')
        subparser.add_argument('--config_path', required=True, type=str, help='path to configuration file')
        subparser.add_argument('--workers', type=int, default=16, help='max workers')
    upload_parser.add_argument('--multipart', action='store_true', dest='multipart', help='use multipart upload')
    args = parser.parse_args()

    s3_connector = AsyncS3Connector.from_yaml(args.config_path)
    local_connector = AsyncLocalConnector()

    s3util = CLI(s3_connector, local_connector)
    if args.action == 'upload':
        error_files = await s3util.upload(local_path=args.local_path, s3_path=args.s3_path,
                                          num_workers=args.workers, multipart=args.multipart)
        print(f'Error files: {error_files}')
    elif args.action == 'download':
        error_files = await s3util.download(local_path=args.local_path, s3_path=args.s3_path, num_workers=args.workers)
        print(f'Error files: {error_files}')
    else:
        raise ValueError(f"invalid action: '{args.action}'")
