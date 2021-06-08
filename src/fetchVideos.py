import logging
from typing import cast
from py2neo import Graph
from py2neo.bulk import create_nodes
import yaml
import os
import json
from acquire.VideoApiGather import query_by_keyword, query_by_relative_id
from process.VideoProcess import process
from models.Video import Video, Channel, Game, fromJson
import datetime
DEFAULT_GAMES = []
DEFAULT_VIDEOS_COUNT = 0
logging.basicConfig(filename='logs//video_gather.log', level=logging.INFO)


def process_game(game_title, expected_video_count):
    graph = Graph(os.environ["NEO4J_BOLT_URL"], auth=(
        os.environ["NEO4J_USERNAMER"], os.environ["NEO4J_PASSWORD"]))
    video_count = cast('int', graph.run(
        "MATCH (:Game {title: $title})--(v:Video) RETURN count(v)", title=game_title).data()[0].get('count(v)'))
    if video_count >= expected_video_count:
        logging.info(f"{game_title} succeded without fetching")
        return
    processed_ids: 'set[str]' = set()
    video_ids: 'list[str]' = []
    miss_search_param = 2
    video_limit = expected_video_count*miss_search_param
    if video_count == 0:
        initialVideos = list(map(fromJson, process(query_by_keyword(game_title, video_limit),
                                                   game_title, processed_ids)))
        for video in initialVideos:
            graph.push(video)
        video_ids = [cast(str, v.videoId) for v in initialVideos]
        video_count = len(video_ids)

    elif video_count < expected_video_count:
        video_ids = cast('list[str]', list(map(lambda i: i.get("v.videoId"), graph.run(
            "MATCH (:Game {title: $title})--(v:Video) RETURN v.videoId", title=game_title).data())))
        processed_ids.update(video_ids)
    while video_count < expected_video_count:
        for videoId in video_ids:
            newVideos = list(map(fromJson, process(query_by_relative_id(videoId, video_limit),
                                                   game_title, processed_ids)))
            for video in newVideos:
                graph.push(video)
            video_count = video_count + len(newVideos)
            if video_count >= expected_video_count:
                break
    logging.info(f"{game_title} succeded")


def main():
    with open('config//games.yaml', 'r', encoding='utf-8') as f:
        parsed_yaml_file = yaml.load(f, Loader=yaml.FullLoader)
    logging.info(f"starting {datetime.datetime.now()}")
    games = parsed_yaml_file.get("games", DEFAULT_GAMES)
    videos_count = parsed_yaml_file.get("videos", DEFAULT_VIDEOS_COUNT)
    for game_title in games:
        process_game(game_title, videos_count)


if __name__ == "__main__":
    main()
