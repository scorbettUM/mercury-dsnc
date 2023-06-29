import time
from typing import List
from .basic_nameservers import BasicNameServers
from .exceptions import NoNameServer
__all__ = [
    'NameServers',
    'NoNameServer',
]


class NameServers(BasicNameServers):

    def __init__(
        self, 
        port: int,
        nameservers: List[str]=[], 
    ):
        super().__init__(
            port,
            nameservers=nameservers
        )

        self._failures = [0] * len(self.data)
        self.ts = 0
        self._update()

    def _update(self):

        if time.time() > self.ts + 60:
            self.ts = time.time()

            self._sorted = list(
                self.data[i] for i in sorted(
                    range(len(self.data)), 
                    key=lambda i: self._failures[i]
                )
            )

            self._failures = [0] * len(self.data)

    def success(self, item):
        self._update()

    def fail(self, item):
        self._update()
        index = self.data.index(item)
        self._failures[index] += 1

    def iter(self):
        if not self.data: 
            raise NoNameServer()
        
        return iter(self._sorted)
