from json_error import JsonError
from timeit import default_timer
from utils.slice import chunked
from typing import Awaitable, Callable
from input_error import InputError
import pickle
from utils.connection import Neo4jConnection
from utils.sync_to_async import run_in_executor
import json
import logging
from dataclasses import asdict
from datetime import datetime, timedelta
import os

from aiogoogle.client import Aiogoogle
from aiogoogle.excs import HTTPError
from utils.chunk_gen import get_new_chunk_iter, get_new_chunk_queue
import queries
from aiokafka.consumer.consumer import AIOKafkaConsumer
import asyncpg
from utils.types import VideoId
from aiokafka.producer.producer import AIOKafkaProducer
from comments_config import from_yaml, CommentsConfig
import yaml
import asyncio
from comment_model import CommentThread, from_json
from prometheus_client import Counter, Gauge, start_http_server, Histogram, Enum, Info
from aiokafka.structs import ConsumerRecord

YOUTUBE_FETCH_QUOTA_COST = 1
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

configuration = Info('microservice_configuration', 'frequency of updates')
app_state = Enum('app_state', 'Information about service state',
                 states=['running', 'waiting_for_quota'])

BUCKETS_FOR_DATABASE_IO = (.1, .25, .5, .75,
                           1.0, 2.5, 5.0, 7.5, 10.0, 15.0, 30.0, 45.0, 60.0, 150.0, 300.0, 450.0, 600.0, 900.0, 1800.0, 3600.0, float("inf"))

replies_io_time = Histogram(
    "replies_io_time", "io time in seconds[s]", ["operation"], buckets=BUCKETS_FOR_DATABASE_IO)
youtube_fetching_time = replies_io_time.labels(operation='youtube_fetching')
neo4j_insert_time = replies_io_time.labels(operation='neo4j_insert')
kafka_produce_time = replies_io_time.labels(operation='kafka_produce')
postgres_fetching_unique_time = replies_io_time.labels(
    operation='postgres_fetching_unique')
postgres_fetching_time = replies_io_time.labels(operation='postgres_fetching')
postgres_insert_time = replies_io_time.labels(operation='postgres_insert')

comments_messages_total = Counter(
    'comments_videos_total', "messages processed from new_videos kafka topic", ['state'])
rejected_messages = comments_messages_total.labels(state='rejected')
repeats_messages = comments_messages_total.labels(state='repeats')
process_messages = comments_messages_total.labels(state='process')
emited_messages = comments_messages_total.labels(state='emited')

insufficient_comments = Counter(
    "insufficient_comments", "comments without channel id")
update_events = Counter("update_events", "updates triggered in program")
quota_usage = Counter("quota_usage", "usage of quota in replies module")

BUCKETS_FOR_COMMENTS_COUNTS = (
    1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, float("inf"))
youtube_fetching_counts = Histogram(
    "youtube_fetching_counts", "counts of replies fetched by youtube per parent", buckets=BUCKETS_FOR_COMMENTS_COUNTS)
BUCKETS_FOR_WAITING_FOR_QUOTA = (.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0, 15.0, 30.0, 45.0, 60.0, 150.0, 300.0,
                                 450.0, 600.0, 900.0, 1800.0, 3600.0, 9000.0, 24*3600.0, 2*24*3600.0, float("inf"))

replies_process = Histogram(
    "comments_process_time", "processing time in seconds[s]", ['type'], buckets=BUCKETS_FOR_WAITING_FOR_QUOTA)
process_message_time = replies_process.labels(type='message')
process_update_time = replies_process.labels(type='update')

inprogress_update = Gauge('inprogress_updates',
                          'inprogress comment_id to fetch childrens')


async def kafka_callback(consumer: AIOKafkaConsumer, callback: Callable[[ConsumerRecord], Awaitable[None]]):
    # Consume messages
    async for msg in consumer:
        await callback(msg)
        await consumer.commit()


async def kafka_callback_bulk(bulk_size: int, consumer: AIOKafkaConsumer, callback: Callable[[list[ConsumerRecord]], Awaitable[None]]):
    # Consume messages
    while True:
        result = await consumer.getmany(timeout_ms=10 * 1000, max_records=bulk_size)
        for tp, messages in result.items():
            if messages:
                await callback(messages)
                # Commit progress only for this partition
                await consumer.commit({tp: messages[-1].offset + 1})


def parse_messages(message):
    return VideoId(message.key.decode("utf-8"))


async def kafka_produce_comment_threads(comment_thread: CommentThread, producer: AIOKafkaProducer):
    if len(comment_thread.replies) > 0:
        with kafka_produce_time.time():
            await producer.send_and_wait("comment_replies", key=get_bytes_video_id(comment_thread), value=get_replies_count(comment_thread))
        emited_messages.inc()


async def process_video_id(video_id: VideoId, pool: asyncpg.Pool, producer: AIOKafkaProducer):
    update = datetime.now()
    try:
        async for comment_threads in get_new_chunk_iter(youtube_fetch_childrens(video_id), 2):
            comments = [asdict(comment_thread.top_level_comment)
                        for comment_thread in comment_threads]
            kafka_emit = [kafka_produce_comment_threads(
                comment_thread, producer) for comment_thread in comment_threads]
            await asyncio.gather(push_to_neo4j(
                queries.all_comment_query, comments), *kafka_emit)
    except InputError:
        update = datetime.now()+TIMEDELTA_WRONG_DATA_UPDATE
    with postgres_insert_time.time():
        async with pool.acquire() as con:
            await con.execute(queries.updated_insert_query, video_id, update)


def process_message_bulk(pool: asyncpg.Pool, producer: AIOKafkaProducer):
    processing = process_message(pool, producer)

    async def f(c: 'list[ConsumerRecord]'):
        await asyncio.gather(*[processing(cr)for cr in c])
    return f


def process_message(pool: asyncpg.Pool, producer: AIOKafkaProducer):
    async def f(c: ConsumerRecord):
        process_messages.inc()
        with process_message_time.time():
            video_id = parse_messages(c)
            with postgres_fetching_unique_time.time():
                async with pool.acquire() as con:
                    exist = (await con.fetch(queries.new_video_query, video_id))[0]
            if not exist:
                repeats_messages.inc()
                return
            await process_video_id(video_id, pool, producer)
    return f


def process_update(pool: asyncpg.Pool, producer: AIOKafkaProducer, frequency: int):
    async def f(_: ConsumerRecord):
        update_events.inc()
        log.info("Update triggered")
        with process_update_time.time():
            if frequency <= 0:
                return
            with postgres_fetching_time.time():
                async with pool.acquire() as con:
                    values = [VideoId(i[0]) for i in await con.fetch(queries.videos_update_query, datetime.now() - timedelta(days=frequency))]
            inprogress_update.set(len(values))
            for videos_id in chunked(10, values):
                await asyncio.gather(*[process_video_id(video_id, pool, producer) for video_id in videos_id])
                inprogress_update.dec(len(videos_id))
    return f


async def youtube_fetch_childrens(video_id: VideoId):
    async with Aiogoogle(api_key=DEVELOPER_KEY) as aiogoogle:
        youtube_api = await aiogoogle.discover('youtube', 'v3')
        req = youtube_api.commentThreads.list(
            part="snippet,replies", videoId=video_id,  # type: ignore
            maxResults=YOUTUBE_COMMENTS_THREAD_CHUNK)  # type: ignore
        while True:
            try:
                _start = default_timer()
                _total_duration = 0.
                full_res = await aiogoogle.as_api_key(req, full_res=True)
                page_number = 0
                rejected = []
                parsed = []
                async for page in full_res:
                    quota_usage.inc(YOUTUBE_FETCH_QUOTA_COST)
                    _total_duration += max(default_timer() - _start, 0)
                    # duration
                    rejected = []
                    parsed: list[CommentThread] = []
                    for item in page['items']:
                        try:
                            parsed.append(from_json(item))
                        except KeyError:
                            rejected.append(item)
                            rejected_messages.inc()
                        except JsonError as err:
                            log.info("Comment without author %s", err.argument)
                            insufficient_comments.inc()
                    if len(rejected) > 0:
                        with open(f'./rejected/comments-{datetime.now()}.json', 'w') as f:
                            json.dump(rejected, f)
                    yield parsed
                    page_number += 1
                    youtube_fetching_counts.observe(
                        (page_number-1)*YOUTUBE_COMMENTS_THREAD_CHUNK + len(rejected) + len(parsed))
                    # start new timer
                    _start = default_timer()

                # insert new final time
                youtube_fetching_time.observe(_total_duration)
                break
            except HTTPError as err:
                if err.res.status_code == 404 and err.res.content['error']['errors'][0]['reason'] == "videoNotFound":
                    log.warning("Incorrect video_id %s",video_id)
                    raise InputError(video_id, "Video id is incorrect")
                elif err.res.status_code == 403 and err.res.content['error']['errors'][0]['reason'] == "commentsDisabled":
                    log.warning("Comments disabled %s",video_id)
                    raise InputError(video_id, "Comments disabled")
                elif err.res.status_code == 403 and err.res.content['error']['errors'][0]['reason'] == "quotaExceeded":
                    await timeout_to_quota_reset()
                raise


async def timeout_to_quota_reset():
    now = datetime.now()
    same_day_update = datetime(
        year=now.year, month=now.month, day=now.day, hour=10)
    next_day_update = datetime(
        year=now.year, month=now.month, day=now.day, hour=10)+timedelta(days=1)
    next_update = same_day_update if now.hour < 10 else next_day_update
    delta = (next_update-datetime.now())
    log.warning("youtube fetch failed and is waiting for %s", delta)
    app_state.state('waiting_for_quota')
    await asyncio.sleep(delta.total_seconds())
    app_state.state('running')


@run_in_executor
def push_to_neo4j(query: str, items: 'list[dict]'):
    if len(items) == 0:
        return
    log.info("neo4j will save %d items", len(items))
    with neo4j_insert_time.time():
        neo4j = Neo4jConnection()
        neo4j.bulk_insert_data(query, items)
        neo4j.close()


def get_bytes_video_id(c: CommentThread): return str.encode(
    c.top_level_comment.comment_id)


def get_replies_count(v: CommentThread): return pickle.dumps(
    (v.top_level_comment.totalReplyCount, list(map(asdict, v.replies))))


async def main(data: CommentsConfig):
    start_http_server(8000)
    configuration.info({'update_frequency': str(data.update_frequency)})
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
        enable_auto_commit=False,
        auto_offset_reset="latest",
        group_id="commentModule"
    )
    producer = AIOKafkaProducer(bootstrap_servers='kafka:9092')
    postgres_pool = asyncpg.create_pool(
        user=user, password=password, database=dbname, host=host, min_size=2)
    _, _, pool, _ = await asyncio.gather(videosConsumer.start(), updateConsumer.start(), postgres_pool, producer.start())
    if not pool:
        raise NotImplementedError("no connctions")
    try:
        # a1 = kafka_callback(videosConsumer, process_message(pool, producer))
        a1 = kafka_callback_bulk(
            5, videosConsumer, process_message_bulk(pool, producer))
        a2 = kafka_callback(updateConsumer, process_update(
            pool, producer, data.update_frequency))
        await asyncio.gather(a1, a2)
    finally:
        await asyncio.gather(videosConsumer.stop(), updateConsumer.stop(), pool.close(), producer.stop())

if __name__ == "__main__":
    with open("config.yaml", 'r', encoding='utf-8') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        asyncio.run(main(from_yaml(config)))
