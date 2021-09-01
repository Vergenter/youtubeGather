from itertools import islice, repeat, takewhile
from typing import Iterable, TypeVar

T = TypeVar('T')


def chunked(size: int, iterable: Iterable[T]) -> 'takewhile[list[T]]':
    """
    Slice an iterable into chunks of n elements
    :type size: int
    :type iterable: Iterable
    :rtype: Iterator
    """
    iterator = iter(iterable)
    return takewhile(bool, (list(islice(iterator, size)) for _ in repeat(None)))
