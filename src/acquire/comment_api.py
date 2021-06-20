import os
import logging
from time import time
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from numpy import empty
DEVELOPER_KEY = os.environ['YOUTUBE_API_KEY_V3']
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'
youtube = build(developerKey=DEVELOPER_KEY,
                serviceName=YOUTUBE_API_SERVICE_NAME, version=YOUTUBE_API_VERSION)
MAX_INT32 = 2147483647


def query_by_video_id(video_id: str, iteration_limit: int = MAX_INT32):
    max_singe_query_results = 100
    youtube_thread = youtube.commentThreads()  # type: ignore pylint: disable=E1101
    request = youtube_thread.list(
        part="snippet,replies",
        videoId=video_id,
        maxResults=max_singe_query_results
    )
    items = empty(500000, dtype=object)
    iteration = 0
    start = time()
    logging.info("[FETCHING] started for %s", video_id)
    quota_exceeded = False
    lastLenght = 0
    try:
        while request is not None and iteration < iteration_limit:
            iteration += 1
            response = request.execute()

            response_items = lastLenght+len(response["items"])
            items[lastLenght:response_items] = response["items"]
            lastLenght = response_items

            if iteration < iteration_limit:
                request = youtube_thread.list_next(request, response)
    except HttpError as err:
        logging.error(str(err))
        quota_exceeded = True
    finally:
        logging.info("fetched %d comments in time: %d",
                     lastLenght, int(time()-start))
        return items[:lastLenght], quota_exceeded


def query_by_comment_id(comment_ids_group: 'list[str]', iteration_limit: int = MAX_INT32):
    max_singe_query_results = 100
    ids = ",".join(comment_ids_group)
    youtube_thread = youtube.comments()  # type: ignore pylint: disable=E1101
    request = youtube_thread.list(
        part="snippet,id",
        parentId=ids,
        maxResults=max_singe_query_results
    )
    items = empty(10000, dtype=object)
    iteration = 0
    start = time()
    logging.info("[FETCHING] started for %s", ids)
    quota_exceeded = False
    lastLenght = 0
    try:
        while request is not None and iteration < iteration_limit:
            iteration += 1
            response = request.execute()
            if len(response["items"]) == 0:
                HttpError(response, "Quota not logged error")
            response_items = lastLenght+len(response["items"])
            items[lastLenght:response_items] = response["items"]
            lastLenght = response_items

            if iteration < iteration_limit:
                request = youtube_thread.list_next(request, response)
    except HttpError as err:
        logging.error(str(err))
        quota_exceeded = True
    finally:
        logging.info("fetched %d comments in time: %d",
                     lastLenght, int(time()-start))
        return items[:lastLenght], quota_exceeded
