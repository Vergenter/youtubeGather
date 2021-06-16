"""video fetching to database"""
import logging
from typing import cast
import os
import datetime

from googleapiclient.errors import HttpError
from py2neo import Graph
import yaml

from acquire.video_api_gather import query_by_keyword, query_by_relative_id
from process.VideoProcess import process
from models.Video import fromJson
DEFAULT_GAMES = []
DEFAULT_VIDEOS_COUNT = 0
logging.basicConfig(filename='logs//video_gather.log', level=logging.INFO)


def process_game(game_title, expected_video_count):
    """process single game"""
    logging.info("%s start", game_title)
    graph = Graph(os.environ["NEO4J_BOLT_URL"], auth=(
        os.environ["NEO4J_USERNAMER"], os.environ["NEO4J_PASSWORD"]))
    video_count = cast('int', graph.run(
        "MATCH (:Game {title: $title})--(v:Video) RETURN count(v)",
        title=game_title).data()[0].get('count(v)'))
    try:
        if video_count >= expected_video_count:
            logging.info("%s succeded without fetching", game_title)
            return
        processed_ids: 'set[str]' = set()
        video_ids: 'list[str]' = []
        miss_search_param = 2
        video_limit = (expected_video_count-video_count)*miss_search_param
        video_limit = (video_limit//50+bool(video_limit % 50))*50

        if video_count == 0:
            initial_videos = list(map(fromJson, process(query_by_keyword(game_title, video_limit),
                                                        game_title, processed_ids)))
            for video in initial_videos:
                graph.push(video)
            video_ids = [cast(str, v.videoId) for v in initial_videos]
            video_count = len(video_ids)

        elif video_count < expected_video_count:
            video_ids = cast('list[str]', list(map(lambda i: i.get("v.videoId"), graph.run(
                "MATCH (:Game {title: $title})--(v:Video) RETURN v.videoId", title=game_title).data())))
            processed_ids.update(video_ids)

            while video_count < expected_video_count:
                for video_id in video_ids:
                    new_videos = list(map(fromJson, process(query_by_relative_id(video_id, video_limit),
                                                            game_title, processed_ids)))
                    for video in new_videos:
                        graph.push(video)
                    video_count = video_count + len(new_videos)
                    if video_count >= expected_video_count:
                        break
            logging.info("%s succeded", game_title)
    except HttpError:
        logging.info("%s failed", game_title)
    except RuntimeError:
        logging.info("%s failed", game_title, exc_info=True)
    except Exception:  # pylint: disable=broad-except
        logging.error("%s failed", game_title, exc_info=True)
        raise


def main():
    """process every game defined in yaml"""
    with open('config//games.yaml', 'r', encoding='utf-8') as config:
        parsed_yaml_file = yaml.load(config, Loader=yaml.FullLoader)
    logging.info("starting %s", datetime.datetime.now())
    games = parsed_yaml_file.get("games", DEFAULT_GAMES)
    videos_count = parsed_yaml_file.get("videos", DEFAULT_VIDEOS_COUNT)
    for game_title in games:
        process_game(game_title, videos_count)


if __name__ == "__main__":
    main()
