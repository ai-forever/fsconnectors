# fsconnectors

Tool for async data transfer with S3.
## Repository structure
```
fsconnectors
├── config.yaml
├── README.md
├── requirements.txt
├── setup.py
└── fsconnectors
    ├── __init__.py
    ├── __main__.py
    └── s3utils.py
```
## Installation
```
pip install git+https://github.com/ai-forever/fsconnectors
```
Or
```
git clone https://github.com/ai-forever/fsconnectors
cd fsconnectors
pip install .
```

## Usage

### Command Line
Upload to S3:
```
python -m fsconnectors upload --s3_path s3://path --local_path local/path --config_path path/to/config.yaml
```
Download from S3:
```
python -m fsconnectors download --s3_path s3://path --local_path local/path --config_path path/to/config.yaml
```
### Python Script
```python
import asyncio
from fsconnectors import S3Util

s3util = S3Util(
    endpoint_url='your_endpoint_url',
    aws_access_key_id='your_aws_access_key_id',
    aws_secret_access_key='your_aws_secret_access_key'
)

# upload to S3
error_files = asyncio.run(s3util.upload(s3_path='s3://path', local_path='local/path'))
# download from S3
error_files = asyncio.run(s3util.download(s3_path='s3://path', local_path='local/path'))
```

### Jupyter Notebook
```python
from fsconnectors import S3Util, parse_config

s3util = S3Util(**parse_config('path/to/config.yaml'))
```
```python
# upload to S3
error_files = await s3util.upload(s3_path='s3://path', local_path='local/path')
```
```python
# download from S3
error_files = await s3util.download(s3_path='s3://path', local_path='local/path')
```

## Configuration file structure
```yaml
endpoint_url: 'your_endpoint_url'
aws_access_key_id: 'your_aws_access_key_id'
aws_secret_access_key: 'your_aws_secret_access_key'
```
