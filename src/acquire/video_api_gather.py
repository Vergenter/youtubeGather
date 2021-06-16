"""search for videos using youtube data api v3"""
import os
import logging
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
DEVELOPER_KEY = os.environ['YOUTUBE_API_KEY_V3']
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'
youtube = build(developerKey=DEVELOPER_KEY,
                serviceName=YOUTUBE_API_SERVICE_NAME, version=YOUTUBE_API_VERSION)

MAX_INT32 = 2147483647


def query_by_keyword(game_title: str, video_limit: int = MAX_INT32):
    """search vieos matching game_title"""
    max_singe_query_results = 50
    youtube_search = youtube.search()  # type: ignore pylint: disable=E1101
    request = youtube_search.list(part="snippet", maxResults=max_singe_query_results,
                                  q=game_title, relevanceLanguage="en", regionCode="US",
                                  type="video")
    return fetching(request, youtube_search, video_limit, max_singe_query_results)


def query_by_relative_id(related_to_video_id: str, video_limit: int = MAX_INT32):
    """search vieos relative to relatedToVideoId"""
    max_singe_query_results = 50
    youtube_search = youtube.search()  # type: ignore pylint: disable=E1101
    request = youtube_search.list(part="snippet", maxResults=max_singe_query_results,
                                  relatedToVideoId=related_to_video_id, relevanceLanguage="en",
                                  regionCode="US", type="video")
    return fetching(request, youtube_search, video_limit, max_singe_query_results)


def fetching(request, youtube_search, video_limit: int, max_singe_query_results: int):
    """fetch selected video count for request"""
    items = []
    iteration = 0
    iteration_limit = int(
        (video_limit+max_singe_query_results-1)//max_singe_query_results)
    while request is not None and iteration < iteration_limit:
        iteration += 1
        try:
            response = request.execute()
            items.extend(response["items"])
        except HttpError as err:
            logging.error(str(err))
            raise
        if iteration > 0:
            request = youtube_search.list_next(request, response)
    logging.info("fetched %d videos", len(items))
    return items
