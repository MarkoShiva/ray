from ray import serve
from typing import List, Dict

from starlette.requests import Request


# __batch_example_start__
@serve.deployment(route_prefix="/increment")
class BatchingExample:
    def __init__(self):
        self.count = 0

    @serve.batch
    async def handle_batch(self, requests: List[Request]) -> List[Dict]:
        return [request.json() for request in requests]

    async def __call__(self, request: Request) -> List[Dict]:
        return await self.handle_batch(request)


serve.run(BatchingExample.bind())
# __batch_example_end__
