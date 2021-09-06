import asyncio

from aiokafka.producer.producer import AIOKafkaProducer
from utils.times import BUCKETS_FOR_DATABASE_IO, BUCKETS_FOR_WAITING_FOR_QUOTA

import yaml
from videos_config import Config, from_yaml
from datetime import datetime, timedelta
from dataclasses import asdict, dataclass
import json
import os
from typing import Any, Awaitable, Callable, Tuple, Union
from utils.sync_to_async import run_in_executor
from prometheus_client import Counter, start_http_server, Histogram, Enum, Info

from aiogoogle.client import Aiogoogle
from aiogoogle.excs import HTTPError
from utils.chunk import chunked
from video_model import Video, from_json, get_empty_video
from neo4j import BoltDriver, GraphDatabase, Neo4jDriver
from utils.types import ChannelId, VideoId
import asyncpg
from aiokafka.consumer.consumer import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord
from aiokafka.errors import CommitFailedError
import queries
from quota_error import QuotaError
import logging

NEO4J = Union[BoltDriver, Neo4jDriver]
LOGGER_NAME = "VIDEOS"
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=os.environ.get("LOGLEVEL", "INFO"), datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger(LOGGER_NAME)

DBNAME = os.environ['POSTGRES_DBNAME']
USER = os.environ['POSTGRES_USER']
PASSWORD = os.environ['POSTGRES_PASSWORD']
HOST = os.environ['POSTGRES_HOST']
NEO4J_BOLT_URL = os.environ["NEO4J_BOLT_URL"]
NEO4J_USERNAME = os.environ["NEO4J_USERNAME"]
NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]
TIMEDELTA_WRONG_DATA_UPDATE = timedelta(weeks=52*1000)
NEW_DATA = datetime.min
DEVELOPER_KEY = os.environ['YOUTUBE_API_KEY_V3']
YOUTUBE_VIDEOS_MAX_CHUNK = 50
YOUTUBE_VIDEOS_PART = "contentDetails,id,liveStreamingDetails,localizations,recordingDetails,snippet,statistics,status,topicDetails"
YOUTUBE_FETCH_QUOTA_COST = 1

configuration = Info('microservice_configuration',
                     'frequency of updates and quota usage')
app_state = Enum('app_state', 'Information about service state',
                 states=['running', 'waiting_for_quota'])

videos_io_time = Histogram(
    "videos_io_time", "io time in seconds[s]", ["operation"], buckets=BUCKETS_FOR_DATABASE_IO)
youtube_fetching_time = videos_io_time.labels(operation='youtube_fetching')
kafka_produce_time = videos_io_time.labels(operation='kafka_produce')
neo4j_insert_time = videos_io_time.labels(operation='neo4j_insert')
postgres_inserting_new_time = videos_io_time.labels(
    operation='postgres_inserting_new')
postgres_fetching_time = videos_io_time.labels(operation='postgres_fetching')
postgres_insert_time = videos_io_time.labels(operation='postgres_insert')

process_messages = Counter(
    'videos_process_messages', "messages processed from new_videos kafka topic")

update_events = Counter("videos_update_events",
                        "updates triggered in program")
quota_usage = Counter("quota_usage", "usage of quota in replies module")
inserted_neo4j = Counter("videos_inserted_neo4j",
                         "videos inserted to neo4j database")
process_update_time = Histogram(
    "videos_process_time", "processing time in seconds[s]", buckets=BUCKETS_FOR_WAITING_FOR_QUOTA)

videos_updated_videos = Counter(
    "videos_updated_videos", "updated videos count", ["state"])
updated_videos = videos_updated_videos.labels(state='success')
rejected_videos = videos_updated_videos.labels(state='rejected')
wrong_videos = videos_updated_videos.labels(state='wrong')

emited_channels = Counter("videos_emited_channels",
                          "emmited channels count to kafka new_channel topic")


async def kafka_callback_bulk(bulk_size: int, consumer: AIOKafkaConsumer, callback: Callable[[list[ConsumerRecord]], Awaitable[None]]):
    # Consume messages
    while True:
        result = await consumer.getmany(timeout_ms=10 * 1000, max_records=bulk_size)
        for tp, messages in result.items():
            if messages:
                await callback(messages)
                # Commit progress only for this partition
                try:
                    await consumer.commit({tp: messages[-1].offset + 1})
                except CommitFailedError:
                    pass


def parse_messages(message):
    return VideoId(message.key)


def process_video_messages(pool: asyncpg.Pool):
    add_update = queries.to_update(NEW_DATA)

    async def f(messages: 'list[ConsumerRecord]'):
        process_messages.inc(len(messages))
        videos_ids = {parse_messages(c) for c in messages}
        with postgres_inserting_new_time.time():
            await pool.executemany(queries.update_insert_new_query, [add_update(x) for x in videos_ids])
    return f


async def update_trigger(frequency_h: float, callback: Callable[[], Awaitable[None]]):
    update_delta = timedelta(hours=frequency_h)
    while True:
        await asyncio.gather(callback(), asyncio.sleep(update_delta.total_seconds()))


async def fetch_videos_from_yt(videos_ids: 'set[VideoId]'):
    result = []
    async with Aiogoogle(api_key=DEVELOPER_KEY) as aiogoogle:
        with youtube_fetching_time.time():
            youtube_api = await aiogoogle.discover('youtube', 'v3')
            req = youtube_api.videos.list(
                part=YOUTUBE_VIDEOS_PART, id=",".join(videos_ids), maxResults=YOUTUBE_VIDEOS_MAX_CHUNK, hl="en_US", regionCode="US")   # type: ignore
            try:
                result = await aiogoogle.as_api_key(req)
                quota_usage.inc(YOUTUBE_FETCH_QUOTA_COST)
            except HTTPError as err:
                if err.res.status_code == 403 and err.res.content['error']['errors'][0]['reason'] == "quotaExceeded":
                    raise QuotaError
                raise
    parsed: list[Video] = []
    rejected = []
    for item in result["items"]:
        try:
            parsed.append(from_json(item))
        except KeyError:
            rejected.append(item)
            rejected_videos.inc()
    if len(rejected) > 0:
        with open(f'./rejected/videos-{datetime.now()}.json', 'w') as f:
            json.dump(rejected, f)
    return parsed


@run_in_executor
def neo4j_query(query: str, items: list[dict], neo4j: NEO4J):
    with neo4j.session() as session:
        session.run(query, {'rows': items})
    inserted_neo4j.inc(len(items))


def get_bytes_channel_id(channel_id: ChannelId): return str.encode(channel_id)


async def kafka_produce_channel_id(channel_id: ChannelId, producer: AIOKafkaProducer):
    with kafka_produce_time.time():
        await producer.send_and_wait("new_channels", key=get_bytes_channel_id(channel_id))
    emited_channels.inc()


async def process_videos_chunk(old_updates: list[Tuple[VideoId, datetime]], pool: asyncpg.Pool, neo4j: NEO4J, producer: AIOKafkaProducer):
    video_ids = {i[0] for i in old_updates}
    try:
        videos = await fetch_videos_from_yt(video_ids)
    except QuotaError:
        return True
    fetched_videos_ids = {video.video_id for video in videos}
    wrong_new_videos = [a[0] for a in old_updates if a[1] ==
                        NEW_DATA and a[0] not in fetched_videos_ids]
    if wrong_new_videos:
        log.warning("Incorrect videos_ids: %s", " ".join(wrong_new_videos))
        wrong_videos.inc(len(wrong_new_videos))
    ok_new_videos = {a[0] for a in old_updates if a[1] ==
                     NEW_DATA and a[0] in fetched_videos_ids}
    wrong_old_videos = {a[0] for a in old_updates if a[1] !=
                        NEW_DATA and a[0] not in fetched_videos_ids}
    tasks = []
    if ok_new_videos:
        parsed_videos = [asdict(
            video) for video in videos if video.video_id in ok_new_videos]
        tasks.append(neo4j_query(
            queries.static_video_query, parsed_videos, neo4j))
    if wrong_old_videos:
        parsed_videos = [get_empty_video(
            wrong_old_video) for wrong_old_video in wrong_old_videos]
        tasks.append(neo4j_query(
            queries.empty_video_insert_query, parsed_videos, neo4j))
    if videos:
        parsed_videos = [asdict(video) for video in videos]
        tasks.append(neo4j_query(
            queries.dynamic_video_query, parsed_videos, neo4j))
    with neo4j_insert_time.time():
        await asyncio.gather(*tasks)
    id_to_update = queries.to_update(datetime.now())
    id_not_to_update = queries.to_update(
        datetime.now()+TIMEDELTA_WRONG_DATA_UPDATE)
    updates = [id_to_update(i) for i in (wrong_old_videos | fetched_videos_ids)]+[
        id_not_to_update(i) for i in wrong_new_videos]
    with postgres_insert_time.time():
        await pool.executemany(queries.update_insert_query, updates)
    await asyncio.gather(*[kafka_produce_channel_id(channel_id, producer) for channel_id in {video.channel_id for video in videos}])
    updated_videos.inc(len(updates))
    return False


def parse_record(record) -> Tuple[VideoId, datetime]:
    return (VideoId(record[0]), record[1])


def process_update(bulk_size: int, config: Config, update_notifier: AIOKafkaConsumer, pool: asyncpg.Pool, neo4j: NEO4J, producer: AIOKafkaProducer):

    async def f():
        update_events.inc()
        log.info("Update triggered")
        quota = config.quota_per_update_limit
        with process_update_time.time():
            while True:
                # take quota into consideration
                curr_fetch = min(quota, bulk_size)*YOUTUBE_VIDEOS_MAX_CHUNK
                with postgres_fetching_time.time():
                    values = await pool.fetch(queries.videos_to_update_query, datetime.now() - timedelta(hours=config.update_frequency_h), curr_fetch)
                parsed_values = [parse_record(value) for value in values]
                if len(parsed_values) == 0:
                    # if no items work is finished
                    break
                quota_exceeded = await asyncio.gather(*[process_videos_chunk(chunk, pool, neo4j, producer) for chunk in chunked(YOUTUBE_VIDEOS_MAX_CHUNK, parsed_values)])
                quota -= sum(1 for exceeded in quota_exceeded if not exceeded)
                # handle Quotaexceeded if even one exceeded
                if quota <= 0 or any(exceeded for exceeded in quota_exceeded):
                    log.warning("quota exceeded")
                    app_state.state('waiting_for_quota')
                    # throw away all old updates
                    await update_notifier.getmany(timeout_ms=0)
                    while True:
                        update = await update_notifier.getmany(timeout_ms=10 * 1000)
                        for tp, messages in update.items():
                            if messages:
                                # if any update break
                                quota = config.quota_per_update_limit
                                app_state.state('running')
                                break
        log.info("Update finished")
    return f


async def main(data: Config):
    start_http_server(8000)
    configuration.info({'update_frequency': str(
        data.update_frequency_h), "quota_per_update_limit": str(data.quota_per_update_limit)})
    log.info("update_frequency_h: %f and quota_per_update_limit: %d",
             data.update_frequency_h, data.quota_per_update_limit)
    videosConsumer = AIOKafkaConsumer(
        'new_videos',
        bootstrap_servers='kafka:9092',
        enable_auto_commit=False,      # Will disable autocommit
        auto_offset_reset="earliest",  # If committed offset not found, start from beginning
        group_id="videoModule",
        key_deserializer=lambda key: key.decode("utf-8") if key else "",
    )
    updateConsumer = AIOKafkaConsumer(
        'updates',
        bootstrap_servers='kafka:9092',
        auto_offset_reset="latest"
    )
    producer = AIOKafkaProducer(bootstrap_servers='kafka:9092')
    postgres_pool = asyncpg.create_pool(
        user=USER, password=PASSWORD, database=DBNAME, host=HOST)
    neo4j = GraphDatabase.driver(
        NEO4J_BOLT_URL, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
    pool, *_ = await asyncio.gather(postgres_pool, videosConsumer.start(), updateConsumer.start(), producer.start())
    if not pool or not neo4j:
        raise NotImplementedError("no connections")
    try:
        tasks = []
        tasks.append(kafka_callback_bulk(100, videosConsumer,
                                         process_video_messages(pool)))
        if data.update_frequency_h > 0 and data.quota_per_update_limit > 0:
            tasks.append(update_trigger(data.update_frequency_h, process_update(
                10, data, updateConsumer, pool, neo4j, producer)))
        await asyncio.gather(*tasks)
    finally:
        await asyncio.gather(videosConsumer.stop(), updateConsumer.stop(), pool.close(), producer.stop())
        neo4j.close()


if __name__ == "__main__":
    log.info("service started")
    with open("config.yaml", 'r', encoding='utf-8') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        asyncio.run(main(from_yaml(config)))
