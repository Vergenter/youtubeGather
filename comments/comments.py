import asyncio
import pickle
from custom_pagination import get_pages
from timeit import default_timer

from aiogoogle.models import Response
from input_error import InputError
from comment_model import CommentThread, QuotaManager, from_json

from aiokafka.producer.producer import AIOKafkaProducer
from utils.times import BUCKETS_FOR_DATABASE_IO, BUCKETS_FOR_WAITING_FOR_QUOTA

import yaml
from comments_config import Config, from_yaml
from datetime import datetime, timedelta
from dataclasses import asdict, dataclass
import json
import os
from typing import Awaitable, Callable, Tuple, Union
from utils.sync_to_async import run_in_executor
from prometheus_client import Counter, start_http_server, Histogram, Enum, Info

from aiogoogle.client import Aiogoogle
from aiogoogle.excs import HTTPError
from utils.chunk import get_new_chunk_iter
from neo4j import BoltDriver, GraphDatabase, Neo4jDriver
from utils.types import CommentId, VideoId
import asyncpg
from aiokafka.consumer.consumer import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord
from aiokafka.errors import CommitFailedError
import queries
import logging

NEO4J = Union[BoltDriver, Neo4jDriver]
LOGGER_NAME = "COMMENTS"
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
YOUTUBE_COMMENTS_THREAD_CHUNK = 100
YOUTUBE_VIDEOS_PART = "contentDetails,id,liveStreamingDetails,localizations,recordingDetails,snippet,statistics,status,topicDetails"
YOUTUBE_FETCH_QUOTA_COST = 1

configuration = Info('microservice_configuration',
                     'frequency of updates and quota usage')
app_state = Enum('app_state', 'Information about service state',
                 states=['running', 'waiting_for_quota'])

comments_io_time = Histogram(
    "comments_io_time", "io time in seconds[s]", ["operation"], buckets=BUCKETS_FOR_DATABASE_IO)
youtube_fetching_time = comments_io_time.labels(operation='youtube_fetching')
kafka_produce_time = comments_io_time.labels(operation='kafka_produce')
neo4j_insert_time = comments_io_time.labels(operation='neo4j_insert')
postgres_inserting_new_time = comments_io_time.labels(
    operation='postgres_inserting_new')
postgres_fetching_time = comments_io_time.labels(operation='postgres_fetching')
postgres_insert_time = comments_io_time.labels(operation='postgres_insert')

process_messages = Counter(
    'comments_process_messages', "messages processed from new_videos kafka topic")

update_events = Counter("comments_update_events",
                        "updates triggered in program")
quota_usage = Counter("quota_usage", "usage of quota in replies module")
inserted_neo4j = Counter("comments_inserted_neo4j",
                         "comments inserted to neo4j database")
process_update_time = Histogram(
    "comments_process_time", "processing time in seconds[s]", buckets=BUCKETS_FOR_WAITING_FOR_QUOTA)

comments_updated_videos = Counter(
    "comments_updated_videos", "updated videos count", ["state"])
updated_videos: Counter = comments_updated_videos.labels(state='success')
rejected_videos: Counter = comments_updated_videos.labels(state='rejected')
wrong_videos: Counter = comments_updated_videos.labels(state='wrong')

updated_comments = Counter(
    "comments_updated_comments", "updated comments count")
emited_parents = Counter("comments_emited_parents",
                         "emmited parent comments count to kafka comment_replies topic")


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
    add_update = queries.to_update_videos(NEW_DATA)

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


def parse_comments(page_items) -> list[CommentThread]:
    parsed = []
    rejected = []
    for item in page_items['items']:
        try:
            parsed.append(from_json(item))
        except KeyError:
            rejected.append(item)
            rejected_videos.inc()
    if len(rejected) > 0:
        with open(f'./rejected/comments-{datetime.now()}.json', 'w') as f:
            json.dump(rejected, f)
    return parsed


async def fetch_comments_from_yt(video_id: VideoId, quota: QuotaManager):
    async with Aiogoogle(api_key=DEVELOPER_KEY) as aiogoogle:
        youtube_api = await aiogoogle.discover('youtube', 'v3')
        req = youtube_api.commentThreads.list(
            part="snippet,replies", videoId=video_id,  # type: ignore
            maxResults=YOUTUBE_COMMENTS_THREAD_CHUNK)  # type: ignore
        while True:
            try:
                await quota.get_quota(YOUTUBE_FETCH_QUOTA_COST)
                with youtube_fetching_time.time():
                    full_res: Response = await aiogoogle.as_api_key(req, full_res=True)

            except HTTPError as err:
                quota_usage.inc(YOUTUBE_FETCH_QUOTA_COST)
                if err.res.status_code == 404 and err.res.content['error']['errors'][0]['reason'] == "videoNotFound":
                    log.warning("Incorrect video_id %s", video_id)
                    raise InputError(video_id, "Video id is incorrect")
                elif err.res.status_code == 403 and err.res.content['error']['errors'][0]['reason'] == "commentsDisabled":
                    log.warning("Comments disabled %s", video_id)
                    raise InputError(video_id, "Comments disabled")
                elif err.res.status_code == 403 and err.res.content['error']['errors'][0]['reason'] == "quotaExceeded":
                    log.error("QuotaExceeded error")
                    quota.quota_exceeded()
                    continue
                raise
            _duration = 0
            _start = default_timer()
            async for page in get_pages(full_res, quota, YOUTUBE_FETCH_QUOTA_COST):
                _duration += max(default_timer() - _start, 0)
                quota_usage.inc(YOUTUBE_FETCH_QUOTA_COST)
                yield parse_comments(page)
                _start = default_timer()
            youtube_fetching_time.observe(_duration)
            break


@run_in_executor
def neo4j_query(query: str, items: list[dict], neo4j: NEO4J):
    with neo4j_insert_time.time():
        with neo4j.session() as session:
            session.run(query, {'rows': items})
        inserted_neo4j.inc(len(items))


def get_bytes_parent_id(c: CommentThread): return str.encode(
    c.top_level_comment.comment_id)


def get_replies(v: CommentThread): return pickle.dumps(
    (v.top_level_comment.totalReplyCount, list(map(asdict, v.replies))))


async def kafka_produce_comment_threads(comment_thread: CommentThread, producer: AIOKafkaProducer):
    if len(comment_thread.replies) > 0:
        with kafka_produce_time.time():
            await producer.send_and_wait("comment_replies", key=get_bytes_parent_id(comment_thread), value=get_replies(comment_thread))
        emited_parents.inc()


async def process_video_id(old_update: Tuple[VideoId, datetime], quota: QuotaManager, pool: asyncpg.Pool, neo4j: NEO4J, producer: AIOKafkaProducer):
    update = datetime.now()
    try:
        async for comment_threads in get_new_chunk_iter(fetch_comments_from_yt(old_update[0], quota), 2):
            comments = [asdict(comment_thread.top_level_comment)
                        for comment_thread in comment_threads]
            kafka_emit = [kafka_produce_comment_threads(
                comment_thread, producer) for comment_thread in comment_threads]
            push_to_neo4j = neo4j_query(
                queries.all_comment_query, comments, neo4j)
            updated_comments.inc(len(comments))
            await push_to_neo4j
            # wait then send to avoid Neo.TransientError.Transaction.DeadlockDetected
            await asyncio.gather(*kafka_emit)
    except InputError:
        if old_update[1] == NEW_DATA:
            update = datetime.now()+TIMEDELTA_WRONG_DATA_UPDATE
            wrong_videos.inc()
    with postgres_insert_time.time():
        async with pool.acquire() as con:
            await con.execute(queries.updated_insert_query, old_update[0], update)
            updated_videos.inc()


def parse_record(record) -> Tuple[VideoId, datetime]:
    return (VideoId(record[0]), record[1])


def process_update(bulk_size: int, config: Config, pool: asyncpg.Pool, neo4j: NEO4J, producer: AIOKafkaProducer):

    async def f():
        update_events.inc()
        log.info("Update triggered")
        quota = QuotaManager(config.quota_per_attempt_limit,
                             config.quota_per_attempt_limit, config.update_attempt_period_h)
        with process_update_time.time():
            while True:
                # take quota into consideration
                curr_fetch = min(quota.read_quota(), bulk_size)
                with postgres_fetching_time.time():
                    values = await pool.fetch(queries.videos_to_update_query, datetime.now() - timedelta(days=config.data_update_period_d), curr_fetch)
                parsed_values = [parse_record(value) for value in values]
                if len(parsed_values) == 0:
                    # if no items work is finished
                    break
                await asyncio.gather(*[process_video_id(update, quota, pool, neo4j, producer) for update in parsed_values])
        log.info("Update finished")
    return f


async def main(data: Config):
    start_http_server(8000)
    configuration.info({
        'data_update_period_d': str(data.data_update_period_d),
        "quota_per_attempt_limit": str(data.quota_per_attempt_limit),
        "update_attempt_period_h": str(data.update_attempt_period_h)
    })
    log.info("data_update_period_d: %f and quota_per_attempt_limit: %d and update_attempt_period_h: %d",
             data.data_update_period_d, data.quota_per_attempt_limit, data.update_attempt_period_h)
    videosConsumer = AIOKafkaConsumer(
        'new_videos',
        bootstrap_servers='kafka:9092',
        enable_auto_commit=False,      # Will disable autocommit
        auto_offset_reset="earliest",  # If committed offset not found, start from beginning
        group_id="commentModule",
        key_deserializer=lambda key: key.decode("utf-8") if key else "",
    )
    producer = AIOKafkaProducer(bootstrap_servers='kafka:9092')
    postgres_pool = asyncpg.create_pool(
        user=USER, password=PASSWORD, database=DBNAME, host=HOST)
    neo4j = GraphDatabase.driver(
        NEO4J_BOLT_URL, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
    pool, *_ = await asyncio.gather(postgres_pool, videosConsumer.start(),  producer.start())
    if not pool or not neo4j:
        raise NotImplementedError("no connections")
    try:
        tasks = []
        tasks.append(kafka_callback_bulk(100, videosConsumer,
                                         process_video_messages(pool)))
        if data.data_update_period_d > 0 and data.quota_per_attempt_limit > 0:
            tasks.append(update_trigger(data.update_attempt_period_h, process_update(
                3, data, pool, neo4j, producer)))
        await asyncio.gather(*tasks)
    finally:
        await asyncio.gather(videosConsumer.stop(),  pool.close(), producer.stop())
        neo4j.close()


if __name__ == "__main__":
    log.info("service started")
    with open("config.yaml", 'r', encoding='utf-8') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        asyncio.run(main(from_yaml(config)))
