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

## Installation
```commandline
git clone https://github.com/ai-forever/fsconnectors
cd fsconnectors/
pip install -r requirements.txt
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
with sc.open('bucket/file.txt', 'wb') as f:
    f.write(b'Hello world!')
entries = sc.listdir('bucket')
```

### Asyncio
> [!IMPORTANT]  
> All async connectors should be used in async context manager with `connect` method
```python
import asyncio
from fsconnectors import AsyncLocalConnector, AsyncS3Connector

async def foo():
    async with AsyncLocalConnector.connect() as lc:
        async with AsyncS3Connector.connect(  # or use from_yaml method
                endpoint_url='endpoint_url',
                aws_access_key_id='aws_access_key_id',
                aws_secret_access_key='aws_secret_access_key'
        ) as sc:
            async with lc.open('./file.txt', 'rb') as lf:
                async with sc.open('bucket/file.txt', 'wb') as sf:
                    await sf.write(await lf.read())

asyncio.run(foo())
```

## API

### Connector
* `open(path, mode)` - open file
  * params:
    * `path: str` - path to file
    * `mode: str` - open mode (only 'rb' and 'wb' for S3 connectors)
    * `multipart: bool = False` - use multipart write (only for S3 connectors)
  * return:
    * `IO` - file like object
* `mkdir(path)` - make directory
  * params:
    * `path: str` - directory path
* `copy(src_path, dst_path, recursive)` - copy file or directory
  * params:
    * `src_path: str` - source path
    * `dst_path: str` - destination path
    * `recursive: bool = False` - recursive
* `move(src_path, dst_path, recursive)` - move file or directory
  * params:
    * `src_path: str` - source path
    * `dst_path: str` - destination path
    * `recursive: bool = False` - recursive
* `remove(path, recursive)` - delete file or directory
  * params:
    * `path: str` - path to file or directory
    * `recursive: bool = False` - recursive
* `listdir(path, recursive)` - list directory content
  * params:
    * `path: str` - directory path
    * `recursive: bool = False` - recursive
  * return:
    * `List[str]` - list of directory contents
* `scandir(path, recursive)` - list directory content with metadata
  * params:
    * `path: str` - directory path
    * `recursive: bool = False` - recursive
  * return:
    * `List[FSEntry]` - list of directory contents with metadata

### FSEntry
File system entry metadata
* `name: str` - entry name
* `path: str` - entry path
* `type: Literal['file', 'dir']` - entry type
* `size: Optional[int] = None` - entry size in bytes (only for files)
* `last_modified: Optional[datetime.datetime] = None` - last modified (only for files)
