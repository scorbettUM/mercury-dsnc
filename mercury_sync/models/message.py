from typing import Optional


class Message:
    
    def __init__(
        self,
        name: str,
        data: Optional[bytes]=None
    ) -> None:
        self.name = name
        self.data = data
