from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import os
import logging
DEVELOPER_KEY = os.environ['YOUTUBE_API_KEY_V3']
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'
youtube = build(developerKey=DEVELOPER_KEY,
                serviceName=YOUTUBE_API_SERVICE_NAME, version=YOUTUBE_API_VERSION)

max_int32 = 2147483647


def query_by_keyword(game_title: str, video_limit: int = max_int32):
    maxSingeQueryResults = 50
    youtubeSearch = youtube.search()  # type: ignore
    request = youtubeSearch.list(part="snippet", maxResults=maxSingeQueryResults,
                                 q=game_title, relevanceLanguage="en", regionCode="US", type="video")
    return fetching(request, youtubeSearch, video_limit, maxSingeQueryResults)


def query_by_relative_id(relatedToVideoId: str, video_limit: int = max_int32):
    maxSingeQueryResults = 50
    youtubeSearch = youtube.search()  # type: ignore
    request = youtubeSearch.list(part="snippet", maxResults=maxSingeQueryResults, relatedToVideoId=relatedToVideoId,
                                 relevanceLanguage="en", regionCode="US", type="video")
    return fetching(request, youtubeSearch, video_limit, maxSingeQueryResults)


def fetching(request, youtubeSearch, video_limit: int, maxSingeQueryResults: int):
    items = []
    iteration = 0
    iteration_limit = int(
        (video_limit+maxSingeQueryResults-1)//maxSingeQueryResults)
    while request is not None and iteration < iteration_limit:
        iteration += 1
        try:
            response = request.execute()
            items.extend(response["items"])
        except HttpError as err:
            logging.error(err._get_reason())
            raise
        if iteration > 0:
            request = youtubeSearch.list_next(request, response)
    logging.info(f"fetched {len(items)} videos")
    return items
