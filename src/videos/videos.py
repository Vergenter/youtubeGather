from dataclasses import asdict
import datetime
import json
from db.connection import Neo4jConnection
from videos.video_model import Video, from_json
from videos.videos_api import VideosManager
from utils.types import VideoId
from videos.videos_config import VideosModuleConfig
import psycopg2
from psycopg2.extras import execute_values
import os
from utils.slice import chunked


dbname = "videodb"
user = os.environ['POSTGRES_ADMIN']
password = os.environ['POSTGRES_ADMIN_PASSWORD']
host = "localhost"

query_to_update = """select video_Id from videos.videos where video_Id not in (select video_Id from videos.videos where update> %s - %s)"""


def update_date(new_date: datetime.date):
    def map_update_model(video_id: VideoId):
        return video_id, new_date
    return map_update_model


static_video_query = '''
UNWIND $rows AS row
MERGE (channel:Channel{channelId: row.channel_id})
MERGE (category:Category{categoryId: row.category_id})
MERGE (defaultLang:Language{language: row.defaultLanguage})
MERGE (defaultAudioLang:Language{language: row.defaultAudioLanguage})
CREATE (video:Video{
    video_id: row.video_id,
    publishedAt: row.publishedAt,
    description: row.description,
    liveBroadcastContent: row.liveBroadcastContent,
    defaultLanguage: row.defaultLanguage,
    defaultAudioLanguage: row.defaultAudioLanguage,
    duration: row.duration,
    is3D: row.is3D,
    ishd: row.ishd,
    licensedContent: row.licensedContent,
    is360degree: row.is360degree,
    cclicense: row.cclicense,
    madeForKids: row.madeForKids
    })
CREATE (video)-[:OWNED_BY]->(channel)
CREATE (video)-[:CATEGORIZED_BY]->(category)
CREATE (video)-[:DEFAULT_LANGUAGE]->(defaultLang)
CREATE (video)-[:DEFAULT_AUDIO_LANGUAGE]->(defaultAudioLang)
FOREACH (language in row.localizations | 
  MERGE (lang:Language{language: row.language})
  CREATE (video)-[:LOCALIZED]->(lang)
)
FOREACH (tag in row.tags | 
  MERGE (t:Tag{tagName: tag})
  CREATE (video)-[:TAGGED]->(t)
)
FOREACH (topic in row.topicCategories | 
  MERGE (t:Topic{url: topic})
  CREATE (video)-[:OF_TOPIC]->(t)
)
FOREACH (region in row.regionRestrictionAllowed | 
  MERGE (r:Region{regionCode: region})
  CREATE (video)-[:ALLOWED_ONLY]->(r)
)
FOREACH (region in row.regionRestrictionBlocked | 
  MERGE (r:Topic{regionCode: region})
  CREATE (video)-[:BLOCKED]->(r)
)
'''

dynamic_video_query = '''
UNWIND $rows AS row
MERGE (video:Video{videoId: row.video_id})
CREATE (videoStatistics:VideoStatistics{
    title: row.title,
    hasCaption: row.hasCaption,
    status: row.status,
    public: row.public,
    embeddable: row.embeddable,
    publicStatsViewable: row.publicStatsViewable,
    viewCount: row.viewCount,
    likeCount: row.likeCount,
    dislikeCount: row.dislikeCount,
    commentCount: row.commentCount
    })
CREATE (videoStatistics)-[:OF{date: row.today}]->(video)
'''


def main(data: VideosModuleConfig):
    today = datetime.date.today()
    if len(data.videosConfig.update) + len(data.channelVideosConfig.update) > 0:
        raise NotImplementedError("That is not implemented!")

    new_video_ids: 'set[VideoId]' = set()
    if len(data.includeConfig.videos) > 0:
        with psycopg2.connect(f"dbname={dbname} user={user} password={password} host={host}") as conn:
            with conn.cursor() as curs:
                execute_values(curs, 'SELECT id FROM (VALUES %s) V(id) EXCEPT SELECT distinct video_Id FROM videos.videos;', list(
                ))
                try:
                    new_video_ids = set(
                        map(lambda x: VideoId(x[0]), curs.fetchall()))
                except psycopg2.ProgrammingError:
                    pass
    # check if need update
    outdated: 'set[VideoId]' = set()
    if data.videosConfig.update_frequency > 0:
        with psycopg2.connect(f"dbname={dbname} user={user} password={password} host={host}") as conn:
            with conn.cursor() as curs:
                curs.execute(query_to_update,
                             (today, data.videosConfig.update_frequency))
                outdated = set(map(lambda x: VideoId(x[0]), curs.fetchall()))
    outdated |= new_video_ids
    results = []
    for videos_ids_chunk in chunked(VideosManager.max_singe_query_results, outdated):
        results += VideosManager.new().list(set(videos_ids_chunk))
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
    conn = Neo4jConnection()
    # 1) push static data
    conn.bulk_insert_data(static_video_query, list(map(asdict, newvideos)))
    # 2) push dynamic data to graph database
    conn.bulk_insert_data(dynamic_video_query, list(map(asdict, parsed)))
    conn.close()

    with psycopg2.connect(f"dbname={dbname} user={user} password={password} host={host}") as conn:
        with conn.cursor() as curs:
            execute_values(curs, 'INSERT INTO "videos"."videos" VALUES %s', list(
                map(update_date(today), map(lambda x: x.video_id, parsed))))

    # send newvideos channels throug kafka

    # 4) perform migration to own database
    # 5) update current database state

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
