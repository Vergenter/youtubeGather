import pickle
from utils.connection import Neo4jConnection
from utils.sync_to_async import run_in_executor
from comment_model import from_json
import json
import logging
from dataclasses import asdict
from datetime import datetime, timedelta
from operator import attrgetter
import os

from aiogoogle.client import Aiogoogle
from aiogoogle.excs import HTTPError
from utils.chunk_gen import get_new_chunk_queue
import queries
from aiokafka.consumer.consumer import AIOKafkaConsumer
import asyncpg
from kafka.structs import TopicPartition
from utils.types import ChannelId, CommentId, ReplyId, VideoId
from aiokafka.producer.producer import AIOKafkaProducer
from comments_config import from_yaml, CommentsConfig
import yaml
import asyncio
from comment_model import CommentThread, Comment, Reply
TIMEDELTA_WRONG_DATA_UPDATE = timedelta(weeks=52*1000)
YOUTUBE_COMMENTS_THREAD_CHUNK = 100
DEVELOPER_KEY = os.environ['YOUTUBE_API_KEY_V3']
dbname = os.environ['POSTGRES_DBNAME']
user = os.environ['POSTGRES_USER']
password = os.environ['POSTGRES_PASSWORD']
host = os.environ['POSTGRES_HOST']
LOGGER_NAME = "COMMENTS"
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
log = logging.getLogger(LOGGER_NAME)


async def kafka_produce(consumer: AIOKafkaConsumer, queue: asyncio.Queue):
    # Consume messages
    async for msg in consumer:
        msg.value = msg.value.decode("utf-8")
        await queue.put(msg)


async def kafka_commit(consumer: AIOKafkaConsumer, msg):
    tp = TopicPartition(msg.topic, msg.partition)
    log.info("kafka commit new offset %s", msg.offset+1)
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
                log.info("From kafka got %d new videoId", len(values))
                for value in values:
                    if value not in new_videos:
                        new_videos.add(value)
                        await outQueue.put(value)


async def process_update(pool: asyncpg.Pool, frequency: int, inQueue: asyncio.Queue, outQueue: asyncio.Queue):
    if frequency > 0:
        async for updates in get_new_chunk_queue(inQueue, 5):
            if len(updates) > 0:
                async with pool.acquire() as con:
                    values = await con.fetch(queries.videos_update_query, datetime.now() - timedelta(days=frequency))
                    for value in values:
                        await outQueue.put(value)


async def youtube_fetch_comments(video_queue: asyncio.Queue[VideoId]):
    while True:
        video_id = await video_queue.get()
        log.info("fetching data from youtube")
        async with Aiogoogle(api_key=DEVELOPER_KEY) as aiogoogle:
            youtube_api = await aiogoogle.discover('youtube', 'v3')
            req = youtube_api.commentThreads.list(
                part="snippet,replies", videoId=video_id,  # type: ignore
                maxResults=YOUTUBE_COMMENTS_THREAD_CHUNK)  # type: ignore
            all_items = []
            error = False
            while True:
                try:
                    full_res = await aiogoogle.as_api_key(req, full_res=True)
                    async for page in full_res:
                        for items in page['items']:
                            all_items.append(items)
                    break
                except HTTPError as err:
                    if err.res.status_code == 404 or err.res.content['error']['errors'][0]['reason'] == "videoNotFound":
                        error = True
                        break
                    elif err.res.status_code == 403 or err.res.content['error']['errors'][0]['reason'] == "commentsDisabled":
                        error = True
                        break
                    elif err.res.status_code != 403 or err.res.content['error']['errors'][0]['reason'] != "quotaExceeded":
                        raise

                    now = datetime.now()
                    same_day_update = datetime(
                        year=now.year, month=now.month, day=now.day, hour=10)
                    next_day_update = datetime(
                        year=now.year, month=now.month, day=now.day, hour=10)+timedelta(days=1)
                    next_update = same_day_update if now.hour < 10 else next_day_update
                    delta = (datetime.now()-next_update)
                    log.debug(
                        "youtube fetch failed %s and is waiting for %s", err.res, delta)
                    await asyncio.sleep(delta.total_seconds())
            rejected = []
            parsed: list[CommentThread] = []
            for item in all_items:
                try:
                    parsed.append(from_json(item))
                except KeyError:
                    rejected.append(item)
            if len(rejected) > 0:
                with open(f'./rejected/comments-{datetime.now()}.json', 'w') as f:
                    json.dump(rejected, f)
            yield parsed, video_id, error


@run_in_executor
def neo4j_blocking_query(neo4j: Neo4jConnection, query: str, videos: 'list[Comment]'):
    neo4j.bulk_insert_data(query, list(map(asdict, videos)))


@run_in_executor
def get_neo4j():
    return Neo4jConnection()


@run_in_executor
def close_neo4j(neo4j: Neo4jConnection):
    neo4j.close()


async def push_to_neo4j(comments: 'list[CommentThread]'):
    if len(comments) == 0:
        return
    log.info("neo4j will save %d comments", len(comments))
    neo4j = await get_neo4j()  # type: ignore
    await neo4j_blocking_query(neo4j, queries.all_comment_query,  # type: ignore
                               list(map(attrgetter("top_level_comment"), comments)))
    await close_neo4j(neo4j)  # type: ignore


async def insert_update(pool: asyncpg.Pool, error: bool, video_id: VideoId):

    update_time = datetime.now()+TIMEDELTA_WRONG_DATA_UPDATE if error else datetime.now()
    async with pool.acquire() as con:
        await con.execute(queries.update_insert_query, video_id, update_time)
        log.info("inserted to postgres videoId")


def get_bytes_video_id(c: CommentThread): return str.encode(
    c.top_level_comment.comment_id)


def get_replies_count(v: CommentThread): return pickle.dumps(
    (v.top_level_comment.totalReplyCount, list(map(asdict, v.replies))))


async def main(data: CommentsConfig):
    log.info("update frequency: %d", data.update_frequency)
    videosConsumer = AIOKafkaConsumer(
        'new_videos',
        bootstrap_servers='kafka:9092',
        enable_auto_commit=False,      # Will disable autocommit
        auto_offset_reset="earliest",  # If committed offset not found, start from beginning
        group_id="commentModule"
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
        async for comments, video_id, error in youtube_fetch_comments(processing_videos):
            if not error:
                await push_to_neo4j(comments)
            await insert_update(pool, error, video_id)
            new_videos.remove(video_id)
            for comment in comments:
                if len(comment.replies) > 0:
                    await producer.send_and_wait("comment_replies", key=get_bytes_video_id(comment), value=get_replies_count(comment))
    finally:
        await asyncio.gather(videosConsumer.stop(), updateConsumer.stop(), pool.close(), producer.stop())

if __name__ == "__main__":
    with open("config.yaml", 'r', encoding='utf-8') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        asyncio.run(main(from_yaml(config)))
