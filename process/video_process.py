"""Process json to fetch video title and language"""
import asyncio
import logging
from typing import Set, Tuple, Any, Callable, TypeVar
from process.language.processor import isVideoInCorrectLanguage
from process.title.processor import getVideoGameTitle

MINIMUM_PROCESS_EFFECIENCY = 0.1


def add_title(item):
    item[1]["gameTitle"] = item[0][0]
    item[1]["gameSubtitle"] = item[0][1]
    return item[1]


def has_snippet(item):
    return item.get("snippet") is not None


def has_correct_label(gameLabel: 'list[str]') -> Callable[[Any], bool]:
    return lambda item: item["gameTitle"] in gameLabel and item["gameSubtitle"] != None


T = TypeVar('T')


def filter_with_logging(predicate: Callable[[T], bool], message: str, data_and_len: Tuple['list[T]', int]):
    result = list(filter(predicate, data_and_len[0]))
    resultLen = len(result)
    logging.info("[PROCESS] %s: %d", message, data_and_len[1]-resultLen)
    return result, resultLen


def process(items, languages, games, processed_ids: Set):
    item_count = len(items)
    logging.info("[PROCESS] processing items: %d", item_count)

    filtered_by_processed_items = filter_with_logging(
        lambda x: x["id"] if type(
            x["id"]) == str else x["id"]["videoId"] not in processed_ids, "repeated items", (items, item_count))

    processed_ids.update(x["id"] if type(
        x["id"]) == str else x["id"]["videoId"]
        for x in filtered_by_processed_items[0])
    filtered_by_having_snippet = filter_with_logging(
        has_snippet, "have no snippet", filtered_by_processed_items)

    filtered_by_language = filter_with_logging(
        lambda x: isVideoInCorrectLanguage(languages, x), "incorrect languages videos", filtered_by_having_snippet)

    loop = asyncio.get_event_loop()
    titles = loop.run_until_complete(asyncio.gather(
        *(getVideoGameTitle(item["id"] if type(
            item["id"]) == str else item["id"]["videoId"]) for item in filtered_by_language[0])))

    filtered_by_game_title = filter_with_logging(
        has_correct_label(games), "incorrect or undefined titles", (list(map(
            add_title, zip(titles, filtered_by_language[0]))), filtered_by_language[1]))
    if filtered_by_game_title[1]/item_count < MINIMUM_PROCESS_EFFECIENCY:
        raise RuntimeError("Too low items go through filtering")
    return filtered_by_game_title[0]
