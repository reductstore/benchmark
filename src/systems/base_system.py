class BaseSystem:
    @classmethod
    async def create(cls):
        raise NotImplementedError

    async def cleanup(self):
        raise NotImplementedError

    async def write_data(self):
        raise NotImplementedError

    async def read_last(self):
        raise NotImplementedError

    async def read_batch(self):
        raise NotImplementedError
