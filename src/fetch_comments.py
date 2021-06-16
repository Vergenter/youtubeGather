import logging
import json
from models.Comment import fromJson
import datetime
from py2neo import Graph
from typing import cast
from acquire.comment_api import query_by_video_id
import os
logging.basicConfig(filename='logs//comments_gather.log', level=logging.INFO)

# filtruje wszystko


def filterJSON(items):

    return filter(lambda json1: json1["snippet"]["topLevelComment"]["snippet"].get("authorChannelId") is not None, items)


def main():
    logging.info("Fetching comments starting %s", datetime.datetime.now())
    graph = Graph(os.environ["NEO4J_BOLT_URL"], auth=(
        os.environ["NEO4J_USERNAMER"], os.environ["NEO4J_PASSWORD"]))
    # make fetching process smarter :D
    video_ids = cast('list[str]', list(map(lambda i: i.get("v.videoId"), graph.run(
        "MATCH (v:Video) where not (v:Video)--(:Comment) RETURN v.videoId").data())))

    logging.info("Fetching comments for %d videos", len(video_ids))

    for video_id in video_ids:
        items = list(map(fromJson, filterJSON(query_by_video_id(video_id))))
        for x in items:
            graph.merge(x)

    logging.info("Fetching comments succeded %s", datetime.datetime.now())


if __name__ == "__main__":
    main()
