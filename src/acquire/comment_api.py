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


def query_by_video_id(video_id: str, iteration_limit: int = MAX_INT32):
    max_singe_query_results = 100
    youtube_thread = youtube.commentThreads()  # type: ignore pylint: disable=E1101
    request = youtube_thread.list(
        part="snippet,replies",
        videoId=video_id,
        maxResults=max_singe_query_results
    )
    items = []
    iteration = 0
    while request is not None and iteration < iteration_limit:
        iteration += 1
        try:
            response = request.execute()
            items.extend(response["items"])
        except HttpError as err:
            logging.error(str(err))
            raise
        if iteration > 0:
            request = youtube_thread.list_next(request, response)
    logging.info("fetched %d comments for %s", len(items), video_id)
    return items
