from dataclasses import asdict
from functools import reduce
import json
from typing import Awaitable, Callable

from aiokafka.structs import ConsumerRecord
from input_error import InputError

from aiogoogle.excs import HTTPError
from replies_model import from_json

from aiogoogle.client import Aiogoogle
from utils.types import CommentId
from replies_model import Reply
from datetime import datetime, timedelta
import logging
from prometheus_client import Counter, Gauge, start_http_server, Histogram, Enum, Info
import os

import queries
from kafka.structs import TopicPartition
from utils.connection import Neo4jConnection
from utils.sync_to_async import run_in_executor
from utils.chunk_gen import get_new_chunk_iter, get_new_chunk_queue

import asyncpg
from replies_model import ParentMessage, from_dict
from aiokafka.consumer.consumer import AIOKafkaConsumer
from replies_config import from_yaml, RepliesConfig
import yaml
import asyncio
import pickle
from timeit import default_timer

YOUTUBE_FETCH_QUOTA_COST = 1
TIMEDELTA_WRONG_DATA_UPDATE = timedelta(weeks=52*1000)
YOUTUBE_COMMENTS_THREAD_CHUNK = 100
DEVELOPER_KEY = os.environ['YOUTUBE_API_KEY_V3']
dbname = os.environ['POSTGRES_DBNAME']
user = os.environ['POSTGRES_USER']
password = os.environ['POSTGRES_PASSWORD']
host = os.environ['POSTGRES_HOST']
LOGGER_NAME = "REPLIES"
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
log = logging.getLogger(LOGGER_NAME)

quota_usage = Counter("quota_usage", "usage of quota in replies module")

inprogress_update = Gauge('inprogress_updates',
                          'inprogress comment_id to fetch childrens')


replies_messages_total = Counter(
    'replies_messages_total', "messages processed from comment_replies kafka topic", ['state'])
rejected_comments = replies_messages_total.labels(state='rejected')

unparsable_messages = replies_messages_total.labels(state='unparsable')

processed_messages = replies_messages_total.labels(state='processed')

update_events = Counter("update_events", "updates triggered in program")

# updated_comments = Counter("updated_comments", "comment updated in program")
app_state = Enum('app_state', 'Information about service state',
                 states=['running', 'waiting_for_quota'])

BUCKETS_FOR_WAITING_FOR_QUOTA = (.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0, 15.0, 30.0, 45.0, 60.0, 150.0, 300.0,
                                 450.0, 600.0, 900.0, 1800.0, 3600.0, 9000.0, 24*3600.0, 2*24*3600.0, float("inf"))

replies_process = Histogram(
    "replies_process_time", "processing time in seconds[s]", ['type'], buckets=BUCKETS_FOR_WAITING_FOR_QUOTA)
process_message_time = replies_process.labels(type='message')
process_update_time = replies_process.labels(type='update')

BUCKETS_FOR_COMMENTS_COUNTS = (
    1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, float("inf"))
youtube_fetching_counts = Histogram(
    "youtube_fetching_counts", "counts of replies fetched by youtube per parent", buckets=BUCKETS_FOR_COMMENTS_COUNTS)

BUCKETS_FOR_DATABASE_IO = (.1, .25, .5, .75,
                           1.0, 2.5, 5.0, 7.5, 10.0, 15.0, 30.0, 45.0, 60.0, 150.0, 300.0, 450.0, 600.0, 900.0, 1800.0, 3600.0, float("inf"))

replies_io_time = Histogram(
    "replies_io_time", "io time in seconds[s]", ["operation"], buckets=BUCKETS_FOR_DATABASE_IO)
youtube_fetching_time = replies_io_time.labels(operation='youtube_fetching')
neo4j_insert_time = replies_io_time.labels(operation='neo4j_insert')
postgres_fetching_time = replies_io_time.labels(operation='postgres_fetching')
postgres_insert_time = replies_io_time.labels(operation='postgres_insert')

configuration = Info('microservice_configuration', 'frequency of updates')


async def kafka_callback(consumer: AIOKafkaConsumer, callback: Callable[[ConsumerRecord], Awaitable[None]]):
    # Consume messages
    async for msg in consumer:
        await callback(msg)
        await consumer.commit()


def process_messages(pool: asyncpg.Pool):
    async def f(c: ConsumerRecord):
        processed_messages.inc()
        with process_message_time.time():
            try:
                parsed = parse_messages(c)
            except pickle.UnpicklingError:
                log.warning("invalid kafka message with offset %d", c.offset)
                unparsable_messages.inc()
                return
            if len(parsed.replies) == 0:
                return
            update = datetime.now()
            if parsed.total_replies > len(parsed.replies):
                try:
                    async for replies in get_new_chunk_iter(youtube_fetch_childrens(parsed.parent_id), 2):
                        await push_to_neo4j(queries.all_comment_query, [asdict(reply) for reply in replies])
                except InputError:
                    update = datetime.now()+TIMEDELTA_WRONG_DATA_UPDATE
            else:
                await push_to_neo4j(queries.all_comment_query, [asdict(reply) for reply in parsed.replies])
            with postgres_insert_time.time():
                async with pool.acquire() as con:
                    await con.execute(queries.updated_insert_query, parsed.parent_id, update)

    return f


def process_update(pool: asyncpg.Pool, frequency: int):
    async def f(_: ConsumerRecord):
        update_events.inc()
        log.info("Update triggered")
        with process_update_time.time():
            if frequency <= 0:
                return
            with postgres_fetching_time.time():
                async with pool.acquire() as con:
                    values = [CommentId(i[0]) for i in await con.fetch(queries.to_update_query, datetime.now() - timedelta(days=frequency))]
            inprogress_update.set(len(values))
            for parent_id in values:
                update = datetime.now()
                try:
                    async for replies in get_new_chunk_iter(youtube_fetch_childrens(parent_id), 2):
                        await push_to_neo4j(queries.all_comment_query, [asdict(reply) for reply in replies])
                except InputError:
                    update = datetime.now()+TIMEDELTA_WRONG_DATA_UPDATE
                with postgres_insert_time.time():
                    async with pool.acquire() as con:
                        await con.execute(queries.updated_insert_query, parent_id, update)
                inprogress_update.dec()
    return f


@run_in_executor
def push_to_neo4j(query: str, items: 'list[dict]'):
    if len(items) == 0:
        return
    log.info("neo4j will save %d items", len(items))
    with neo4j_insert_time.time():
        neo4j = Neo4jConnection()
        neo4j.bulk_insert_data(query, items)
        neo4j.close()


def parse_messages(message):
    key = message.key.decode("utf-8")
    value = pickle.loads(message.value)
    return ParentMessage(key, value[0], list(map(from_dict, value[1])))


async def timeout_to_quota_reset():
    TIME_DRIFT = timedelta(minutes=1)
    now = datetime.now()
    same_day_update = datetime(
        year=now.year, month=now.month, day=now.day, hour=7)
    next_day_update = same_day_update+timedelta(days=1)
    next_update = same_day_update if now.hour < 10 else next_day_update
    delta = (next_update-datetime.now())+TIME_DRIFT
    log.warning("youtube fetch failed and is waiting for %s", delta)
    app_state.state('waiting_for_quota')
    await asyncio.sleep(delta.total_seconds())
    app_state.state('running')


async def youtube_fetch_childrens(parent_id: CommentId):
    async with Aiogoogle(api_key=DEVELOPER_KEY) as aiogoogle:
        youtube_api = await aiogoogle.discover('youtube', 'v3')
        req = youtube_api.comments.list(
            part="snippet,id", parentId=parent_id, maxResults=YOUTUBE_COMMENTS_THREAD_CHUNK)  # type: ignore
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
                    parsed: list[Reply] = []
                    for item in page['items']:
                        try:
                            parsed.append(from_json(item))
                        except KeyError:
                            rejected.append(item)
                            rejected_comments.inc()
                    if len(rejected) > 0:
                        with open(f'./rejected/replies-{datetime.now()}.json', 'w') as f:
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
                if err.res.status_code == 404 and err.res.content['error']['errors'][0]['reason'] == "commentNotFound":
                    log.warning("Incorrect parent_id")
                    raise InputError(parent_id, "Comment id is incorrect")
                elif err.res.status_code == 403 and err.res.content['error']['errors'][0]['reason'] == "quotaExceeded":
                    await timeout_to_quota_reset()
                else:
                    raise


async def main(data: RepliesConfig):
    start_http_server(8000)
    configuration.info({'update_frequency': str(data.update_frequency)})
    log.info("update frequency: %d", data.update_frequency)
    replyConsumer = AIOKafkaConsumer(
        'comment_replies',
        bootstrap_servers='kafka:9092',
        enable_auto_commit=False,      # Will disable autocommit
        auto_offset_reset="earliest",  # If committed offset not found, start from beginning
        group_id="reply2Module"
    )
    updateConsumer = AIOKafkaConsumer(
        'updates',
        bootstrap_servers='kafka:9092',
        enable_auto_commit=False,
        auto_offset_reset="latest",
        group_id="replyModule"
    )
    postgres_pool = asyncpg.create_pool(
        user=user, password=password, database=dbname, host=host, min_size=2)
    _, _, pool = await asyncio.gather(updateConsumer.start(), replyConsumer.start(), postgres_pool)
    if not pool:
        raise NotImplementedError("no connctions")
    try:

        a1 = kafka_callback(replyConsumer, process_messages(pool))
        a2 = kafka_callback(updateConsumer, process_update(
            pool, data.update_frequency))
        await asyncio.gather(a1, a2)
    finally:
        await asyncio.gather(updateConsumer.stop(), replyConsumer.stop(), pool.close())


if __name__ == "__main__":
    with open("config.yaml", 'r', encoding='utf-8') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        asyncio.run(main(from_yaml(config)))
