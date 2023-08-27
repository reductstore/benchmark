from abc import ABC, abstractmethod
from typing import List


class BaseSystem(ABC):
    @classmethod
    @abstractmethod
    async def create(cls):
        """Create the necessary resources for the system."""
        pass

    @abstractmethod
    async def cleanup(self) -> None:
        """Clean up any resources."""
        pass

    @abstractmethod
    async def write_data(self, data: bytes, timestamp_ns: int) -> None:
        """Write data with a timestamp."""
        pass

    @abstractmethod
    async def read_last(self) -> bytes:
        """Read the last piece of data."""
        pass

    @abstractmethod
    async def read_batch(self, start_ns: int) -> List[bytes]:
        """Read a batch of data starting from a specific timestamp."""
        pass
