# fsconnectors
File system connectors.

## Contents
* [Installation](#installation)
* [Usage](#usage)
  * [LocalConnector](#localconnector)
  * [S3Connector](#s3connector)
  * [Asyncio](#s3connector)
* [API](#api)
  * [Connector](#connector)
  * [FSEntry](#fsentry)
* [CLI](#CLI)
  * [Upload](#upload)
  * [Download](#download)

## Installation
```commandline
git clone https://github.com/ai-forever/fsconnectors
cd fsconnectors/
pip install .
```

## Usage

### LocalConnector
```python
from fsconnectors import LocalConnector

lc = LocalConnector()
with lc.open('./file.txt', 'w') as f:
    f.write('Hello world!')
entries = lc.listdir('.')
```

### S3Connector
```python
from fsconnectors import S3Connector

sc = S3Connector.from_yaml('path/to/config.yaml')
with sc.open('bucket/file.txt', 'w') as f:
    f.write('Hello world!')
entries = sc.listdir('bucket')
```

### Asyncio
> [!IMPORTANT]  
> All async connectors should be used in async context manager with `connect` method
```python
import asyncio
from fsconnectors import AsyncLocalConnector, AsyncS3Connector

async def foo():
    async with AsyncLocalConnector().connect() as lc:
        async with AsyncS3Connector(  # or use from_yaml method
                endpoint_url='endpoint_url',
                aws_access_key_id='aws_access_key_id',
                aws_secret_access_key='aws_secret_access_key'
        ).connect() as sc:
            async with lc.open('./file.txt', 'rb') as lf:
                async with sc.open('bucket/file.txt', 'wb') as sf:
                    await sf.write(await lf.read())

asyncio.run(foo())
```

## API

### Connector
* `open(path, mode)` - open file
  * parameters:
    * `path: str` - path to file
    * `mode: str` - open mode
    * `multipart: bool = False` - use multipart writer (only for S3 connectors)
  * returns:
    * `Union[ContextManager, AsyncContextManager]` - file-like object
* `mkdir(path)` - make directory
  * parameters:
    * `path: str` - directory path
* `copy(src_path, dst_path, recursive)` - copy file or directory
  * parameters:
    * `src_path: str` - source path
    * `dst_path: str` - destination path
    * `recursive: bool = False` - recursive
* `move(src_path, dst_path, recursive)` - move file or directory
  * parameters:
    * `src_path: str` - source path
    * `dst_path: str` - destination path
    * `recursive: bool = False` - recursive
* `remove(path, recursive)` - delete file or directory
  * parameters:
    * `path: str` - path to file or directory
    * `recursive: bool = False` - recursive
* `listdir(path, recursive)` - list directory content
  * parameters:
    * `path: str` - directory path
    * `recursive: bool = False` - recursive
  * returns:
    * `List[str]` - list of directory contents
* `scandir(path, recursive)` - list directory content with metadata
  * parameters:
    * `path: str` - directory path
    * `recursive: bool = False` - recursive
  * returns:
    * `List[FSEntry]` - list of directory contents with metadata

### FSEntry
File system entry metadata
* `name: str` - entry name
* `path: str` - entry path
* `type: Literal['file', 'dir']` - entry type
* `size: Optional[int] = None` - entry size in bytes (only for files)
* `last_modified: Optional[datetime.datetime] = None` - last modified (only for files)

## CLI

### Upload
```
python -m fsconnectors upload [-h] --s3_path S3_PATH --local_path LOCAL_PATH --config_path CONFIG_PATH [--workers WORKERS] [--multipart]

optional arguments:
  -h, --help                show this help message and exit
  --s3_path S3_PATH         S3 folder path
  --local_path LOCAL_PATH   local folder path
  --config_path CONFIG_PATH path to configuration file
  --workers WORKERS         max workers
  --multipart               use multipart upload
```

### Download
```
python -m fsconnectors download [-h] --s3_path S3_PATH --local_path LOCAL_PATH --config_path CONFIG_PATH [--workers WORKERS]

optional arguments:
  -h, --help                show this help message and exit
  --s3_path S3_PATH         S3 folder path
  --local_path LOCAL_PATH   local folder path
  --config_path CONFIG_PATH path to configuration file
  --workers WORKERS         max workers
```