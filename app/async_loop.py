import asyncio
from threading import Thread

loop = asyncio.new_event_loop()


def run_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


t = Thread(target=run_loop, args=(loop,))
t.start()


def call_async(coro):
    asyncio.run_coroutine_threadsafe(coro, loop)
