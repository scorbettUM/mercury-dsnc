import inspect


async def is_generator(data=[]):
    for d in data:
        yield d


