import asyncio


async def get_new_chunk_gen(queue: asyncio.Queue, timeout_s=0.6, max_chunk_size=2):
    while True:
        items = []
        while True:
            try:
                item = await asyncio.wait_for(queue.get(), timeout_s)
            except asyncio.TimeoutError:
                if len(items) > 0:
                    break
            else:
                queue.task_done()
                items.append(item)
                if len(items) >= max_chunk_size:
                    break
        yield items
