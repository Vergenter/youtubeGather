import logging
from typing import Set, Tuple, Any, Callable, TypeVar
T = TypeVar('T')


def filter_with_logging(predicate: Callable[[T], bool], message: str, data_and_len: Tuple['list[T]', int]):
    result = list(filter(predicate, data_and_len[0]))
    resultLen = len(result)
    logging.info("[PROCESS] %s: %d", message, data_and_len[1]-resultLen)
    return result, resultLen


def has_channelId(json):
    return json["snippet"]["topLevelComment"]["snippet"].get("authorChannelId") is not None


def process_commandThread(items):
    item_count = len(items)
    logging.info("[PROCESS] processing items: %d", item_count)

    filtered_by_author_ids = filter_with_logging(
        has_channelId, "no authorChannelId", (items, item_count))

    return filtered_by_author_ids[0]


def process_command(items):
    item_count = len(items)
    logging.info("[PROCESS] processing items: %d", item_count)

    filtered_by_author_ids = filter_with_logging(
        lambda x: x["snippet"].get("authorChannelId") is not None, "no authorChannelId", (items, item_count))

    return filtered_by_author_ids[0]
