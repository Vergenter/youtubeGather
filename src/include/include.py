import os
from include.include_config import include
from include.include_videos_api import GIncludeVideoManager, fetch_videos
import ray


def main(data: include):
    # result = fetch_videos.remote(set(data.videos))
    ids = [fetch_videos.remote({i}) for i in set(data.videos)]
    result = ray.get(ids)
    print(result)
