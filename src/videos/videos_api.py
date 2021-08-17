from dataclasses import dataclass
import os
from typing import Any
from google_api.pooling import APIConnector
import googleapiclient.discovery

from utils.types import VideoId
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'
DEVELOPER_KEY = os.environ['YOUTUBE_API_KEY_V3']
MAX_SINGLE_QUERY_RESULTS = 50


@dataclass
class VideosManager:
    max_singe_query_results = 50
    api: APIConnector
    resource: Any

    @classmethod
    def new(cls):
        api = APIConnector.new()
        service = googleapiclient.discovery.build(
            serviceName=YOUTUBE_API_SERVICE_NAME,
            version=YOUTUBE_API_VERSION,
            developerKey=DEVELOPER_KEY,
            cache_discovery=False,
        )
        resource = service.videos()  # type: ignore pylint: disable=E1101
        return cls(api=api, resource=resource)

    def list(self, videos_id: 'set[VideoId]'):
        items: 'list[dict]' = []
        ids = ",".join(videos_id)
        request = self.resource.list(part="contentDetails,id,liveStreamingDetails,localizations,recordingDetails,snippet,statistics,status,topicDetails",
                                     id=ids,
                                     maxResults=MAX_SINGLE_QUERY_RESULTS,
                                     hl="en_US",
                                     regionCode="US")
        while request is not None:
            response = self.api.execute(request)
            items.extend(response["items"])
            request = self.resource.list_next(request, response)
        return items
