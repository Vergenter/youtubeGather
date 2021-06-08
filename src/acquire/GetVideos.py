from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import os
import json
import time
import logging
import asyncio
from .GetGameTitle import getVideoGameTitle
from mcache import filecache, DAY
from datetime import date
logging.basicConfig(filename='logs//youtube_gather.log', level=logging.DEBUG)
DEVELOPER_KEY = os.environ['YOUTUBE_API_KEY_V3']
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'
youtube = build(developerKey=DEVELOPER_KEY, serviceName=YOUTUBE_API_SERVICE_NAME, version=YOUTUBE_API_VERSION,
                )


@filecache(lifetime=DAY)
def getItems(query, iteration_limit):
    maxSingeQueryResults = 50
    youtubeSearch = youtube.search()  # type: ignore
    request = youtubeSearch.list(part="snippet", maxResults=maxSingeQueryResults,
                                 q=query, relevanceLanguage="en", regionCode="US", type="video")
    items = []
    iteration = 0
    start = time.time()
    maxData = iteration_limit * maxSingeQueryResults
    while request is not None and iteration < iteration_limit:
        iteration += 1
        try:
            response = request.execute()
            items.extend(response["items"])
            if iteration == 1:
                maxData = min(response["pageInfo"]
                              ["totalResults"], iteration_limit*50)
            logging.info(f"Fetched {len(items)}/{maxData} items")
        except HttpError as err:
            logging.error(err._get_reason())
            break
        if iteration > 0:
            request = youtubeSearch.list_next(request, response)
    logging.debug(f"Requests took {time.time() - start}")
    return items


def add_titles(items):
    loop = asyncio.get_event_loop()
    titles = loop.run_until_complete(asyncio.gather(
        *(getVideoGameTitle(item["id"]["videoId"]) for item in items)))
    for i in range(len(items)):
        items[i]["gameTitle"] = titles[i][0]
        items[i]["gameSubtitle"] = titles[i][1]
    return items


def save_items(items, filename):
    with open('data//'+filename+'.json', 'w', encoding='utf-8') as f:
        json.dump(items, f, ensure_ascii=False, indent=4)


def getVideos(gameTitle):
    logging.debug(f"Current date {date.today()}")
    logging.debug(f"Start for {gameTitle}")
    items = getItems(gameTitle, 20)
    logging.info(f"Got {len(items)} items")  # type: ignore
    start = time.time()
    items = add_titles(items)
    logging.debug(f"Getting game titles took {time.time() - start}")
    save_items(items, gameTitle)
