import asyncio
from pickle import NONE
from typing import TypeVar, AsyncGenerator, AsyncIterator

X = TypeVar('X')


async def get_new_chunk_queue(queue: asyncio.Queue[X], timeout_s: float = 0.6, max_chunk_size: int = 2) -> AsyncGenerator[list[X], None]:
    while True:
        items: 'list[X]' = []
        while True:
            try:
                item = await asyncio.wait_for(asyncio.shield(queue.get()), timeout_s)
            except asyncio.TimeoutError:
                if len(items) > 0:
                    break
            else:
                queue.task_done()
                items.append(item)
                if len(items) >= max_chunk_size:
                    break
        yield items
T = TypeVar('T')


# async generator, needs python 3.6
async def get_new_chunk_iter(it: AsyncIterator[list[T]], timeout_s: float = 0.6, max_chunk_size: int = 2) -> AsyncGenerator[list[T], None]:
    try:
        nxt = asyncio.ensure_future(it.__anext__())
        while True:
            items: 'list[T]' = []
            while True:
                try:
                    item = await asyncio.wait_for(asyncio.shield(nxt), timeout_s)
                    nxt = asyncio.ensure_future(it.__anext__())
                except asyncio.TimeoutError:
                    if len(items) > 0:
                        break
                else:
                    items += item
                    if len(items) >= max_chunk_size:
                        break
            yield items
    except StopAsyncIteration:
        pass
