import functools
import asyncio
from typing import Awaitable, Callable, TypeVar, overload


T = TypeVar('T')
T1 = TypeVar('T1')
T2 = TypeVar('T2')
T3 = TypeVar('T3')
R = TypeVar('R')


@overload
def run_in_executor(f:  Callable[[], R]) -> Callable[[], Awaitable[R]]:
    ...


@overload
def run_in_executor(f:  Callable[[T], R]) -> Callable[[T], Awaitable[R]]:
    ...


@overload
def run_in_executor(f:  Callable[[T, T1], R]) -> Callable[[T, T1], Awaitable[R]]:
    ...


@overload
def run_in_executor(f:  Callable[[T, T1, T2], R]) -> Callable[[T, T1, T2], Awaitable[R]]:
    ...


@overload
def run_in_executor(f:  Callable[[T, T1, T2, T3], R]) -> Callable[[T, T1, T2, T3], Awaitable[R]]:
    ...


def run_in_executor(f:  Callable[..., R]) -> Callable[..., Awaitable[R]]:
    @functools.wraps(f)
    async def inner(*args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: f(*args, **kwargs))
    return inner
