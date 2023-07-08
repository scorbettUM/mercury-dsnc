import json
from pydantic import (
    StrictStr,
    StrictInt,
    Json
)
from typing import Dict, Optional, Union, Literal, List
from .message import Message


class HTTPMessage(Message):
    protocol: StrictStr
    path: Optional[StrictStr]
    method: Optional[
        Literal[
            "GET", 
            "POST",
            "HEAD",
            "OPTIONS", 
            "PUT", 
            "PATCH", 
            "DELETE"
        ]
    ]
    status: Optional[StrictInt]
    status_message: Optional[StrictStr]
    params: Dict[StrictStr, StrictStr]={}
    headers: Dict[StrictStr, StrictStr]={}
    data: Optional[Union[Json, StrictStr]]

    def prepare_response(self):

        request: List[str] = [
            f'HTTP/1.1 {self.status} {self.error}'
        ]

        request.extend([
            f'{key}: {value}' for key, value in self.headers.items()
        ])

        encoded_data = None
        if isinstance(self.data, Message):
            encoded_data = json.dumps(self.data.to_data()).encode()

        elif self.data:
            encoded_data = self.data.encode()
            content_length = len(encoded_data)
            
            request.append(
                f'content-length: {content_length}'
            )

        else:
            request.append(
                'content-length: 0'
            )

        request.append('\r\n')
        if encoded_data:
            request.append(encoded_data)


        encoded_request = '\r\n'.join(request)

        return encoded_request.encode()