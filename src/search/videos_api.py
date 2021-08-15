from dataclasses import dataclass
from google_api.pooling import APIConnector
import googleapiclient.discovery
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'


@dataclass
class GRelatedVideoManager:
    max_singe_query_results = 50
    api: APIConnector
    resource: googleapiclient.discovery.Resource

    @classmethod
    def new(cls, credentials) -> GVideoManager:
        api = APIConnector.new(credentials)
        service = googleapiclient.discovery.build(
            serviceName=YOUTUBE_API_SERVICE_NAME,
            version=YOUTUBE_API_VERSION,
            credentials=credentials,
            cache_discovery=False,
        )
        resource = service.search()  # type: ignore pylint: disable=E1101
        return cls(api=api, resource=resource)

    def list(self, related_to_video_id: str) -> list[dict]:
        items = []
        request = self.resource.list(part="snippet", maxResults=self.max_singe_query_results,
                                     relatedToVideoId=related_to_video_id, relevanceLanguage="en",
                                     regionCode="US", type="video")
        while request is not None:
            response = self.api.execute(request)
            items.extend(response["items"])
            request = self.resource.list_next(request, response)
        return items
