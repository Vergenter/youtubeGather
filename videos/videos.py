from dataclasses import asdict
import datetime
import json
from db.connection import Neo4jConnection
from videos.video_model import Video, from_json
from videos.videos_api import VideosManager, MAX_SINGLE_QUERY_RESULTS
from utils.types import VideoId
from videos.videos_config import VideosModuleConfig
import psycopg2
from psycopg2.extras import execute_values
import os
from utils.slice import chunked
from videos.queries import static_video_query, dynamic_video_query, query_to_update

dbname = "videodb"
user = os.environ['POSTGRES_ADMIN']
password = os.environ['POSTGRES_ADMIN_PASSWORD']
host = "localhost"


def update_date(new_date: datetime.date):
    def map_update_model(video_id: VideoId):
        return video_id, new_date
    return map_update_model


def main(data: VideosModuleConfig):

    today = datetime.date.today()
    if len(data.videosConfig.update) + len(data.channelVideosConfig.update) > 0:
        raise NotImplementedError("That is not implemented!")

    new_video_ids: 'set[VideoId]' = set()
    if len(data.includeConfig.videos) > 0:
        with psycopg2.connect(f"dbname={dbname} user={user} password={password} host={host}") as conn:
            with conn.cursor() as curs:
                execute_values(curs, 'SELECT id FROM (VALUES %s) V(id) EXCEPT SELECT distinct video_Id FROM videos.videos;', list(
                    map(lambda x: (x,), data.includeConfig.videos)))
                new_video_ids = set(
                    map(lambda x: VideoId(x[0]), curs.fetchall()))
    outdated: 'set[VideoId]' = set()
    if data.videosConfig.update_frequency > 0:
        with psycopg2.connect(f"dbname={dbname} user={user} password={password} host={host}") as conn:
            with conn.cursor() as curs:
                curs.execute(query_to_update,
                             (today, data.videosConfig.update_frequency))
                outdated = set(map(lambda x: VideoId(x[0]), curs.fetchall()))
    merged = new_video_ids | outdated
    results = []
    vm = VideosManager.new()
    for videos_ids_chunk in chunked(MAX_SINGLE_QUERY_RESULTS, merged):
        results += vm.list(set(videos_ids_chunk))
    print(vm.quota_usage)
    parsed: 'list[Video]' = []
    errous = []
    for result in results:
        try:
            parsed.append(from_json(result))
        except KeyError:
            errous.append(result)
    if len(errous) > 0:
        with open('errous.json', 'w+') as f:
            json.dump(errous, f)

    newvideos = list(filter(lambda x: x.video_id in new_video_ids, parsed))
    # 1) push static data
    neo4j = Neo4jConnection()
    neo4j.bulk_insert_data(static_video_query, list(map(asdict, newvideos)))
    # 2) push dynamic data to graph database
    neo4j.bulk_insert_data(dynamic_video_query, list(map(asdict, parsed)))
    neo4j.close()

    with psycopg2.connect(f"dbname={dbname} user={user} password={password} host={host}") as conn:
        with conn.cursor() as curs:
            execute_values(curs, 'INSERT INTO "videos"."videos" VALUES %s', list(
                map(update_date(today), map(lambda x: x.video_id, parsed))))

    # send newvideos channels throug kafka

    # channel ids for channels videos baching -> channel videos ids
    # check if input channels should be updated -> new? channel ids
    # read channels videos data from database to update videos -> channel ids

    # video ids for update batching -> video state
    # read videos data from database to update -> videos ids
    # videos ids for need to be updated batching -> video ids
    # input new? videos ids
    # channel new? videos ids

    # video state -> add new videos update date to own database

    # new? videos ids to -> new videos ids
    # input new? videos ids
    # channel new? videos ids

    # new videos ids send through kafka

    # new videos ids + video state => add to graph database new static video data
    # !!! possible simplify connecting new and old

    # video state => add to graph database new dynamic video data

    # send through kafka new channel ids
    # new videos ids query to db -> new channels ids
