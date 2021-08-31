import logging
from dataclasses import asdict
import json
import kafka_send
from video_model import from_json
from videos_config import from_yaml
from videos_config import VideosConfig
from video_model import Video

from aiogoogle.excs import HTTPError
from aiokafka.producer.producer import AIOKafkaProducer
from utils.connection import Neo4jConnection
from utils.sync_to_async import run_in_executor
from datetime import datetime, timedelta
import queries
from aiogoogle.client import Aiogoogle
from utils.chunk_gen import get_new_chunk_queue
from kafka.structs import TopicPartition
import yaml
from aiokafka import AIOKafkaConsumer
import asyncio
from utils.types import VideoId
import asyncpg
import os
from operator import attrgetter

LOGGER_NAME = "VIDEOS"
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
log = logging.getLogger(LOGGER_NAME)

TIMEDELTA_WRONG_DATA_UPDATE = timedelta(weeks=52*1000)
DEVELOPER_KEY = os.environ['YOUTUBE_API_KEY_V3']
YOUTUBE_VIDEOS_MAX_CHUNK = 50
dbname = os.environ['POSTGRES_DBNAME']
user = os.environ['POSTGRES_USER']
password = os.environ['POSTGRES_PASSWORD']
host = os.environ['POSTGRES_HOST']


async def kafka_produce(consumer: AIOKafkaConsumer, queue: asyncio.Queue):
    # Consume messages
    async for msg in consumer:
        msg.value = msg.value.decode("utf-8")
        await queue.put(msg)


async def kafka_commit(consumer: AIOKafkaConsumer, msg):
    tp = TopicPartition(msg.topic, msg.partition)
    log.debug("kafka commit new offset %s", msg.offset+1)
    await consumer.commit({tp: msg.offset+1})


async def kafka_commit_from_messages(consumer, values, messages):
    try:
        first_new_index = next(index for index, x in enumerate(
            messages) if x.value in values)
        if first_new_index > 0:
            await kafka_commit(consumer, messages[first_new_index-1])
        return False
    except StopIteration:
        await kafka_commit(consumer, messages[-1])
        return True


async def process_filter(consumer: AIOKafkaConsumer, pool: asyncpg.Pool, inQueue: asyncio.Queue, outQueue: asyncio.Queue, new_videos: 'set[VideoId]'):
    kafka_commit = True
    async for messages in get_new_chunk_queue(inQueue, 5, 1000):
        if len(messages) > 0:
            async with pool.acquire() as con:
                # need to parse args to ids
                values = {VideoId(i["id"]) for i in await con.fetch(queries.new_videos_query, list(map(VideoId, map(attrgetter("value"), messages))))}
                if kafka_commit:
                    kafka_commit = await kafka_commit_from_messages(consumer, values, messages)
                log.debug("From kafka got %d new videoId", len(values))
                for value in values:
                    if value not in new_videos:
                        new_videos.add(value)
                        await outQueue.put(value)


async def process_update(pool: asyncpg.Pool, frequency: int, inQueue: asyncio.Queue, outQueue: asyncio.Queue):
    log.info("update triggered")
    if frequency > 0:
        async for updates in get_new_chunk_queue(inQueue, 5):
            if len(updates) > 0:
                async with pool.acquire() as con:
                    values = await con.fetch(queries.videos_update_query, datetime.now() - timedelta(days=frequency))
                    for value in values:
                        await outQueue.put(value)


async def fetch_videos_from_yt(videos_ids: 'list[VideoId]'):
    async with Aiogoogle(api_key=DEVELOPER_KEY) as aiogoogle:
        youtube_api = await aiogoogle.discover('youtube', 'v3')
        req = youtube_api.videos.list(
            part="contentDetails,id,liveStreamingDetails,localizations,recordingDetails,snippet,statistics,status,topicDetails", id=",".join(videos_ids), maxResults=YOUTUBE_VIDEOS_MAX_CHUNK, hl="en_US", regionCode="US")  # type: ignore
        parsed: list[Video] = []
        result = {"items": []}
        while True:
            try:
                result = await aiogoogle.as_api_key(req)
                break
            except HTTPError as err:
                if err.res.status_code != 403 or err.res.content['error']['errors'][0]['reason'] != "quotaExceeded":
                    raise
                now = datetime.now()
                same_day_update = datetime(
                    year=now.year, month=now.month, day=now.day, hour=10)
                next_day_update = datetime(
                    year=now.year, month=now.month, day=now.day, hour=10)+timedelta(days=1)
                next_update = same_day_update if now.hour < 10 else next_day_update
                delta = (datetime.now()-next_update)
                log.warning(
                    "youtube fetch failed and is waiting for %s", delta)
                await asyncio.sleep(delta.total_seconds())

        rejected = []
        for item in result["items"]:
            try:
                parsed.append(from_json(item))
            except KeyError:
                rejected.append(item)
        if len(rejected) > 0:
            with open(f'./rejected/videos-{datetime.now()}.json', 'w') as f:
                json.dump(rejected, f)
        return parsed


@run_in_executor
def neo4j_blocking_query(neo4j: Neo4jConnection, query: str, videos: 'list[Video]'):
    neo4j.bulk_insert_data(query, list(map(asdict, videos)))


@run_in_executor
def get_neo4j():
    return Neo4jConnection()


@run_in_executor
def close_neo4j(neo4j: Neo4jConnection):
    neo4j.close()


async def push_to_neo4j(videos: 'list[Video]', new_videos: 'set[VideoId]'):
    if len(videos) == 0:
        return
    videos_to_create = [
        video for video in videos if video.video_id in new_videos]
    log.info("neo4j will save %d videos", len(videos))
    neo4j = await get_neo4j()  # type: ignore
    if len(videos_to_create) > 0:
        await asyncio.gather(neo4j_blocking_query(neo4j, queries.static_video_query, videos_to_create),  # type: ignore
                             neo4j_blocking_query(neo4j, queries.dynamic_video_query, videos_to_create))
    else:
        await neo4j_blocking_query(
            neo4j, queries.dynamic_video_query, videos_to_create)  # type: ignore

    await close_neo4j(neo4j)  # type: ignore


async def insert_update(pool: asyncpg.Pool, videos: 'list[Video]', potentialy_wrong_videos_ids: 'list[VideoId]'):
    if len(videos) == 0 and len(potentialy_wrong_videos_ids) == 0:
        return
    videos_to_update = list(map(queries.video_to_update, videos))
    if len(videos) != len(potentialy_wrong_videos_ids):
        error_update_time = datetime.now() + TIMEDELTA_WRONG_DATA_UPDATE
        videos_to_update.extend(map(queries.to_update(error_update_time), {
            i.video_id for i in videos}.symmetric_difference(potentialy_wrong_videos_ids)))
        log.info("number of errous video_id %d", len(
            potentialy_wrong_videos_ids)-len(videos))
    async with pool.acquire() as con:
        await con.executemany(queries.update_insert_query, videos_to_update)
        log.debug("inserted to postgres %d updated", len(videos))


async def main(data: VideosConfig):
    log.info("update frequency: %d", data.update_frequency)
    videosConsumer = AIOKafkaConsumer(
        'new_videos',
        bootstrap_servers='kafka:9092',
        enable_auto_commit=False,      # Will disable autocommit
        auto_offset_reset="earliest",  # If committed offset not found, start from beginning
        group_id="videoModule"
    )
    updateConsumer = AIOKafkaConsumer(
        'updates',
        bootstrap_servers='kafka:9092',
        enable_auto_commit=True,
        auto_offset_reset="latest",
    )
    producer = AIOKafkaProducer(bootstrap_servers='kafka:9092')
    postgres_pool = asyncpg.create_pool(
        user=user, password=password, database=dbname, host=host, min_size=2)
    _, _, pool, _ = await asyncio.gather(videosConsumer.start(), updateConsumer.start(), postgres_pool, producer.start())
    if not pool:
        raise NotImplementedError("no connctions")
    try:
        input_videos = asyncio.Queue()
        update_videos = asyncio.Queue()
        processing_videos = asyncio.Queue()
        new_videos: 'set[VideoId]' = set()
        asyncio.create_task(kafka_produce(videosConsumer, input_videos))
        asyncio.create_task(kafka_produce(updateConsumer, update_videos))
        asyncio.create_task(process_filter(videosConsumer,
                                           pool, input_videos, processing_videos, new_videos))
        asyncio.create_task(process_update(
            pool, data.update_frequency, update_videos, processing_videos))
        # long waiting chunk for youtube
        # add all items to set
        async for videos_ids in get_new_chunk_queue(processing_videos, 30, YOUTUBE_VIDEOS_MAX_CHUNK):
            # timeout even for one day
            log.debug("fetching data from youtube")
            videos = await fetch_videos_from_yt(videos_ids)
            await push_to_neo4j(videos, new_videos)
            await insert_update(pool, videos, videos_ids)
            new_videos.difference_update(videos_ids)
            await kafka_send.kafka_send(producer, "new_channels", map(
                kafka_send.channel_parser, videos))

    finally:
        await asyncio.gather(videosConsumer.stop(), updateConsumer.stop(), pool.close(), producer.stop())


if __name__ == "__main__":
    log.info("service started")
    with open("config.yaml", 'r', encoding='utf-8') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        asyncio.run(main(from_yaml(config)))
