from utils.slice import chunked
from include.include_config import include
from include.include_videos_api import MAX_SINGLE_QUERY_RESULTS, fetch_videos
import ray


def main(data: include):
    # result = fetch_videos.remote(set(data.videos))
    ids = [fetch_videos.remote(set(i)) for i in chunked(
        MAX_SINGLE_QUERY_RESULTS, data.videos)]
    result = ray.get(ids)
    print(result)
