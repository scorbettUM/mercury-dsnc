from mercury_sync.models.message import Message
from typing import TypeVar, Tuple, Generic, TypeVarTuple


T = TypeVar('T', bound=Message)


Call = Tuple[int, T]