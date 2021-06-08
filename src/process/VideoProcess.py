from typing import Set, Tuple, Any, Callable, TypeVar
from .language.LanguageProcessor import isVideoEnglish
from .title.TitleProcessor import getVideoGameTitle
import asyncio
import logging


def add_title(item):
    item[1]["gameTitle"] = item[0][0]
    item[1]["gameSubtitle"] = item[0][1]
    return item[1]


def has_snippet(item):
    return item.get("snippet") is not None


def has_correct_label(gameLabel) -> Callable[[Any], bool]:
    return lambda item: item["gameTitle"] == gameLabel and item["gameSubtitle"] != None


T = TypeVar('T')


def filter_with_logging(predicate: Callable[[T], bool], message: str, data_and_len: 'Tuple[list[T],int]'):
    result = list(filter(predicate, data_and_len[0]))
    resultLen = len(result)
    logging.info(f"[PROCESS] {message}: {data_and_len[1]-resultLen}")
    return result, resultLen


def process(items, gameLabel, processed_ids: Set):
    item_count = len(items)
    logging.info(f"[PROCESS] processing items: {item_count}")

    filtered_by_processed_items = filter_with_logging(
        lambda x: x["id"]["videoId"] not in processed_ids, "repeated items", (items, item_count))
    [item for item in items if item["id"]
                                   ["videoId"] not in processed_ids]
    processed_ids.update(x["id"]["videoId"]
                         for x in filtered_by_processed_items[0])
    filtered_by_having_snippet = filter_with_logging(
        has_snippet, "have no snippet", filtered_by_processed_items)

    filtered_by_language = filter_with_logging(
        isVideoEnglish, "non english videos", filtered_by_having_snippet)

    loop = asyncio.get_event_loop()
    titles = loop.run_until_complete(asyncio.gather(
        *(getVideoGameTitle(item["id"]["videoId"]) for item in filtered_by_language[0])))

    filtered_by_game_title = filter_with_logging(
        has_correct_label(gameLabel), "incorrect or undefined titles", (list(map(
            add_title, zip(titles, filtered_by_language[0]))), filtered_by_language[1]))

    return filtered_by_game_title[0]
