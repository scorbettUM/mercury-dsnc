from mercury_sync.service_discovery.dns.server.entries import DNSEntry
from typing import List
from .message import Message


class Registration(Message):
    records: List[DNSEntry]
