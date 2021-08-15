from dataclasses import dataclass
import os

import ray
from google_api.pooling import APIConnector
import googleapiclient.discovery
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'
DEVELOPER_KEY = os.environ['YOUTUBE_API_KEY_V3']
MAX_SINGLE_QUERY_RESULTS = 50


@ray.remote
def fetch_videos(videos_id: 'set[str]'):
    api = APIConnector.new()
    service = googleapiclient.discovery.build(
        serviceName=YOUTUBE_API_SERVICE_NAME,
        version=YOUTUBE_API_VERSION,
        cache_discovery=False,
        developerKey=DEVELOPER_KEY
    )
    resource = service.videos()  # type: ignore pylint: disable=E1101

    items = []
    ids = ",".join(videos_id)
    request = resource.list(part="snippet",  # type: ignore pylint: disable=E1101
                                 id=ids,
                                 maxResults=MAX_SINGLE_QUERY_RESULTS)
    while request is not None:
        response = api.execute(request)
        items.extend(response["items"])
        # type: ignore pylint: disable=E1101
        request = resource.list_next(request, response)
    return items
