import os
from include.include_config import include
from include.include_videos_api import GIncludeVideoManager, fetch_videos
import ray


def main(data: include):
    ids = [fetch_videos.remote(set(list(i))) for i in set(data.videos)]
    ready_ids, _ = ray.wait(ids, num_returns=len(ids))
    result = ray.get(ready_ids)
    print(result)
