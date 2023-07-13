from pydantic import (
    BaseModel
)
from typing import (
    Dict, 
    Union
)


class Response:
    def __init__(
        self,
        headers: Dict[str, str]={},
        data: Union[BaseModel, str, None]=None
    ):
        self.headers = headers
        self.data = data
