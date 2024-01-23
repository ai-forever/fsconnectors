import asyncio
from .s3utils import main


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
