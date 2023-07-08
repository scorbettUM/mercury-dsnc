import json
from pydantic import (
    BaseModel,
    Json
)
from typing import (
    Dict, 
    Union, 
    List, 
    TypeVar, 
    Generic,
    Optional
)


T = TypeVar('T', bound=BaseModel)


class Request(Generic[T]):

    def __init__(
        self,
        path: str,
        method: str,
        query: str,
        raw: List[bytes]
    ) -> None:
        
        self.path = path
        self.method = method
        self._query = query

        self._headers: Dict[str, str] = {}
        self._params: Dict[str, str] = {}
        self._data: Union[str, Json, None] = None

        self._raw = raw
        self._data_line_idx = -1


    @property
    def headers(self):

        if self._data_line_idx == -1:
            header_lines = self._raw[1:]
            data_line_idx = 0

            for header_line in header_lines:

                if header_line == b'':
                    data_line_idx += 1
                    break
                
                key, value = header_line.decode().split(
                    ':', 
                    maxsplit=1
                )

                self._headers[key.lower()] = value.strip()

                data_line_idx += 1
            
            self._data_line_idx = data_line_idx + 1

        return self._headers
    
    @property
    def params(self):
        
        if len(self._params) < 1:
            params = self._query.split('&')

            for param in params:
                key, value = param.split('=')

                self._params[key] = value

        return self._params

    @property
    def body(self):
        
        headers = self._headers

        if self._data is None:
            self._data = b''.join(self._raw[self._data_line_idx:]).strip()

            if headers.get('content-type') == 'application/json':
                self._data = json.loads(self._data)

        return self._data
    
    def data(self, model: Optional[T]=None):
        data = self.body

        if isinstance(data, dict) and model:
            return model(**data)
        
        return data

