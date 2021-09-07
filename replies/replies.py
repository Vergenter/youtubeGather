import asyncio
import functools
import pickle

from neo4j.exceptions import ConstraintError
from quota_manager import QuotaManager
from custom_pagination import get_pages
from timeit import default_timer

from aiogoogle.models import Response
from input_error import InputError
from replies_model import Reply, ParentMessage, from_dict, from_json

from utils.times import BUCKETS_FOR_DATABASE_IO, BUCKETS_FOR_WAITING_FOR_QUOTA

import yaml
from replies_config import Config, from_yaml
from datetime import datetime, timedelta
from dataclasses import asdict
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
import operator
NEO4J = Union[BoltDriver, Neo4jDriver]
LOGGER_NAME = "REPLIES"
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
YOUTUBE_FETCH_QUOTA_COST = 1

configuration = Info('microservice_configuration',
                     'frequency of updates and quota usage')
app_state = Enum('app_state', 'Information about service state',
                 states=['running', 'waiting_for_quota'])

replies_io_time = Histogram(
    "replies_io_time", "io time in seconds[s]", ["operation"], buckets=BUCKETS_FOR_DATABASE_IO)
youtube_fetching_time = replies_io_time.labels(operation='youtube_fetching')
neo4j_insert_time = replies_io_time.labels(operation='neo4j_insert')
postgres_inserting_new_time = replies_io_time.labels(
    operation='postgres_inserting_new')
postgres_upserting_new_time = replies_io_time.labels(
    operation='postgres_upserting_new')
postgres_fetching_time = replies_io_time.labels(operation='postgres_fetching')
postgres_insert_time = replies_io_time.labels(operation='postgres_insert')

replies_messages_total = Counter(
    'replies_process_messages', "messages processed from comment_replies kafka topic", ['state'])
process_messages = replies_messages_total.labels(state='process')
unparsable_messages = replies_messages_total.labels(state='unparsable')
update_events = Counter("replies_update_events",
                        "updates triggered in program")
quota_usage = Counter("quota_usage", "usage of quota in replies module")
inserted_neo4j = Counter("replies_inserted_neo4j",
                         "replies inserted to neo4j database")

replies_process = Histogram(
    "replies_process_time", "processing time in seconds[s]", ['type'], buckets=BUCKETS_FOR_WAITING_FOR_QUOTA)
process_message_time = replies_process.labels(type='message')
process_update_time = replies_process.labels(type='update')

replies_updated_parents = Counter(
    "replies_updated_parents", "updated comment parents count", ["state"])
updated_parents: Counter = replies_updated_parents.labels(state='success')
rejected_parents: Counter = replies_updated_parents.labels(state='rejected')
wrong_parents: Counter = replies_updated_parents.labels(state='wrong')

updated_comments = Counter(
    "replies_updated_comments", "updated comments count")


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
    try:
        value = pickle.loads(message.value)
    except pickle.UnpicklingError:
        log.error("invalid kafka message with offset %d and key %s",
                  message.offset, message.key)
        unparsable_messages.inc()
        return None
    return ParentMessage(message.key, value[0], list(map(from_dict, value[1])))


async def handle_full_updates(full_update: dict[tuple[CommentId, datetime], list[Reply]], pool: asyncpg.Pool, neo4j: NEO4J):
    with postgres_upserting_new_time.time():
        new_parents = await queries.upsert(list(full_update.keys()), pool)
    parsed_new_parents = [parse_record(
        value) for value in new_parents]
    replies = functools.reduce(
        operator.iconcat, [full_update.get(x, []) for x in parsed_new_parents], [])
    # need testing if it works!!!
    if replies:
        await neo4j_query(queries.all_comment_query, [asdict(reply) for reply in replies], neo4j)


async def handle_partial_updates(data: list[tuple[CommentId, datetime]], pool: asyncpg.Pool):
    with postgres_inserting_new_time.time():
        await pool.executemany(queries.update_insert_new_query, data)


def process_video_messages(pool: asyncpg.Pool, neo4j: NEO4J):
    add_update = queries.to_update(NEW_DATA)

    async def f(messages: 'list[ConsumerRecord]'):
        process_messages.inc(len(messages))
        with process_message_time.time():
            parent_messages: list[ParentMessage] = [parsed for parsed in (
                parse_messages(c) for c in messages) if parsed is not None and len(parsed.replies) != 0]
            full_update = {(parent_message.parent_id, parent_message.replies[0].update)
                            : parent_message.replies for parent_message in parent_messages if parent_message.total_replies < 6}
            partial_update = {
                parent_message.parent_id for parent_message in parent_messages if parent_message.total_replies > 5}
            tasks = []
            if full_update:
                tasks.append(handle_full_updates(full_update, pool, neo4j))
            if partial_update:
                data = [add_update(x) for x in partial_update]
                tasks.append(handle_partial_updates(data, pool))
            await asyncio.gather(*tasks)
    return f


async def update_trigger(frequency_h: float, callback: Callable[[], Awaitable[None]]):
    update_delta = timedelta(hours=frequency_h)
    while True:
        await asyncio.gather(callback(), asyncio.sleep(update_delta.total_seconds()))


def parse_comments(page_items) -> list[Reply]:
    parsed = []
    rejected = []
    for item in page_items['items']:
        try:
            parsed.append(from_json(item))
        except KeyError:
            rejected.append(item)
            rejected_parents.inc()
    if len(rejected) > 0:
        with open(f'./rejected/replies-{datetime.now()}.json', 'w') as f:
            json.dump(rejected, f)
    return parsed


async def fetch_comments_from_yt(parent_id: CommentId, quota: QuotaManager):
    async with Aiogoogle(api_key=DEVELOPER_KEY) as aiogoogle:
        youtube_api = await aiogoogle.discover('youtube', 'v3')
        req = youtube_api.comments.list(
            part="snippet,id", parentId=parent_id, maxResults=YOUTUBE_COMMENTS_THREAD_CHUNK)  # type: ignore
        while True:
            try:
                await quota.get_quota(YOUTUBE_FETCH_QUOTA_COST)
                with youtube_fetching_time.time():
                    full_res: Response = await aiogoogle.as_api_key(req, full_res=True)

            except HTTPError as err:
                quota_usage.inc(YOUTUBE_FETCH_QUOTA_COST)
                if err.res.status_code == 404 and err.res.content['error']['errors'][0]['reason'] == "commentNotFound":
                    log.warning("Incorrect comment_id %s", parent_id)
                    raise InputError(parent_id, "Incorrect comment_id")
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
def neo4j_query(query: str, items: list[dict], neo4ja: NEO4J):
    with neo4j_insert_time.time():
        with neo4ja.session() as session:
            session.run(query, {'rows': items})
        inserted_neo4j.inc(len(items))


async def process_parent_id(old_update: Tuple[CommentId, datetime], quota: QuotaManager, pool: asyncpg.Pool, neo4j: NEO4J):
    update = datetime.now()
    try:
        async for replies in get_new_chunk_iter(fetch_comments_from_yt(old_update[0], quota), 2):
            updated_comments.inc(len(replies))
            await neo4j_query(queries.all_comment_query, [asdict(reply) for reply in replies], neo4j)
    except InputError:
        if old_update[1] == NEW_DATA:
            update = datetime.now()+TIMEDELTA_WRONG_DATA_UPDATE
            wrong_parents.inc()
    with postgres_insert_time.time():
        async with pool.acquire() as con:
            await con.execute(queries.updated_insert_query, old_update[0], update)
            updated_parents.inc()


def parse_record(record) -> Tuple[CommentId, datetime]:
    return (CommentId(record[0]), record[1])


def process_update(bulk_size: int, config: Config, pool: asyncpg.Pool, neo4j: NEO4J, quota: QuotaManager):

    async def f():
        update_events.inc()
        log.info("Update triggered")
        with process_update_time.time():
            while True:
                # take quota into consideration
                curr_fetch = min(quota.read_quota(), bulk_size)
                with postgres_fetching_time.time():
                    values = await pool.fetch(queries.parents_to_update_query, datetime.now() - timedelta(days=config.data_update_period_d), curr_fetch)
                parsed_values = [parse_record(value) for value in values]
                if len(parsed_values) == 0:
                    # if no items work is finished
                    break
                await asyncio.gather(*[process_parent_id(update, quota, pool, neo4j) for update in parsed_values])
        log.info("Update finished")
    return f


async def main(data: Config):
    start_http_server(8000)
    configuration.info({
        "data_update_period_d": str(data.data_update_period_d),
        "quota_per_attempt_limit": str(data.quota_per_attempt_limit),
        "update_attempt_period_h": str(data.update_attempt_period_h)
    })
    log.info("data_update_period_d: %f and quota_per_attempt_limit: %d and update_attempt_period_h: %d",
             data.data_update_period_d, data.quota_per_attempt_limit, data.update_attempt_period_h)
    replyConsumer = AIOKafkaConsumer(
        'comment_replies',
        bootstrap_servers='kafka:9092',
        enable_auto_commit=False,      # Will disable autocommit
        auto_offset_reset="earliest",  # If committed offset not found, start from beginning
        group_id="replyModule",
        key_deserializer=lambda key: key.decode("utf-8") if key else "",
    )
    postgres_pool = asyncpg.create_pool(
        user=USER, password=PASSWORD, database=DBNAME, host=HOST)
    neo4j = GraphDatabase.driver(
        NEO4J_BOLT_URL, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
    pool, *_ = await asyncio.gather(postgres_pool, replyConsumer.start())
    quota = QuotaManager(data.quota_per_attempt_limit,
                         data.quota_per_attempt_limit, data.update_attempt_period_h)
    if not pool or not neo4j:
        raise NotImplementedError("no connections")
    try:
        tasks = []
        tasks.append(kafka_callback_bulk(20, replyConsumer,
                                         process_video_messages(pool, neo4j)))
        if data.data_update_period_d > 0 and data.quota_per_attempt_limit > 0 and data.update_attempt_period_h > 0:
            tasks.append(update_trigger(data.update_attempt_period_h, process_update(
                3, data, pool, neo4j, quota)))
        await asyncio.gather(*tasks)
    finally:
        await asyncio.gather(replyConsumer.stop(),  pool.close())
        neo4j.close()


if __name__ == "__main__":
    log.info("service started")
    with open("config.yaml", 'r', encoding='utf-8') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        asyncio.run(main(from_yaml(config)))
