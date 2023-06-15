import asyncio
from mercury_sync.service import Service
from mercury_sync.service.test_message import TestMessage
from mercury_sync.hooks.server import server
from mercury_sync.hooks.stream import stream
from typing import List


class TestService(Service[TestMessage, TestMessage]):

    data = ["hello", "test"]

    @server()
    async def example(
        self,
        shard_id: int,
        data: bytes
    ):

        for response in self.data:
            response_message = TestMessage(
                message=response
            )

            yield response_message

    @stream('example', direct=True)
    async def message_example(self, messages: List[str]):
        for message in messages:
            yield TestMessage(
                message=message
            )


async def run(test_client: TestService, test_server: TestService):

    await test_server.connect(
        TestMessage(
            host='0.0.0.0',
            port=1125
        )
    )


    await test_client.connect(
        TestMessage(
            host='0.0.0.0',
            port=1123
        )
    )


    async for response in test_client.message_example(["Hello world!", "test"]):
        print(response)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    test_server = TestService(
        host='0.0.0.0',
        port=1123
    )

    test_server.start()

    test_client = TestService(
        host='0.0.0.0',
        port=1125
    )

    test_client.start()
    
    loop.run_until_complete(
        run(
            test_client,
            test_server
        )
    )