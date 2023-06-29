from typing import Dict, Union


class HTTPResponse:
    def __init__(
        self, 
        url,
        status: int, 
        message: str, 
        headers: Dict[str, str], 
        data: Union[str, None], 
    ):
        
        self.url = url
        self.status = status
        self.message = message
        self.headers = headers
        self.data = data