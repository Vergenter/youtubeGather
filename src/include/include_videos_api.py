from dataclasses import dataclass
import os

import ray
from google_api.pooling import APIConnector
import googleapiclient.discovery
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'
DEVELOPER_KEY = os.environ['YOUTUBE_API_KEY_V3']


@ray.remote
def fetch_videos(videos_id: 'set[str]'):
    max_singe_query_results = 50
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
    request = self.resource.list(part="snippet",  # type: ignore pylint: disable=E1101
                                 id=ids,
                                 maxResults=self.max_singe_query_results)
    while request is not None:
        response = api.execute(request)
        items.extend(response["items"])
        # type: ignore pylint: disable=E1101
        request = resource.list_next(request, response)
    return items


@dataclass
class GIncludeVideoManager:

    api: APIConnector
    resource: googleapiclient.discovery.Resource

    @classmethod
    def new(cls):
        api = APIConnector.new()
        service = googleapiclient.discovery.build(
            serviceName=YOUTUBE_API_SERVICE_NAME,
            version=YOUTUBE_API_VERSION,
            cache_discovery=False,
            developerKey=DEVELOPER_KEY
        )
        resource = service.videos()  # type: ignore pylint: disable=E1101
        return cls(api=api, resource=resource)

    def list(self, video_ids: 'set[str]') -> list[dict]:
        items = []
        ids = ",".join(video_ids)
        request = self.resource.list(part="snippet",  # type: ignore pylint: disable=E1101
                                     id=ids,
                                     maxResults=self.max_singe_query_results)
        while request is not None:
            response = self.api.execute(request)
            items.extend(response["items"])
            # type: ignore pylint: disable=E1101
            request = self.resource.list_next(request, response)
        return items
