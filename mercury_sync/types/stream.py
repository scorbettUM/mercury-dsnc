from mercury_sync.models.message import Message
from typing import AsyncIterable, TypeVar
from .call import Call


T = TypeVar('T', bound=Message)


Stream = AsyncIterable[Call[T]]
