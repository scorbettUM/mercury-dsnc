import asyncio
from asyncio.streams import (
    StreamReader,
    StreamWriter
)


class Connection:
    def __init__(
        self, 
        reader: StreamReader, 
        writer: StreamWriter, 
        timer: asyncio.Future=None
    ):
        self.reader = reader
        self.writer = writer
        self.timer = timer
