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
from operator import attrgetter
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


async def kafka_callback(consumer: AIOKafkaConsumer, callback: Callable[[ConsumerRecord], Awaitable[None]]):
    # Consume messages
    async for msg in consumer:
        await callback(msg)
        await consumer.commit()


def process_messages(pool: asyncpg.Pool):
    async def f(c: ConsumerRecord):
        parsed = parse_messages(c)
        if len(parsed.replies) == 0:
            return
        update = datetime.now()
        if parsed.total_replies > len(parsed.replies):
            try:
                async for replies in get_new_chunk_iter(youtube_fetch_childrens(parsed.parent_id), 2):
                    await push_to_neo4j(replies)
            except InputError:
                update = datetime.now()+TIMEDELTA_WRONG_DATA_UPDATE
        else:
            await push_to_neo4j(parsed.replies)
        async with pool.acquire() as con:
            await con.execute(queries.updated_insert_query, parsed.parent_id, update)
    return f


def process_update(pool: asyncpg.Pool, frequency: int):
    async def f(_: ConsumerRecord):
        log.info("Update triggered")
        if frequency <= 0:
            return
        async with pool.acquire() as con:
            values = await con.fetch(queries.to_update_query, datetime.now() - timedelta(days=frequency))
        for parent_id in values:
            update = datetime.now()
            try:
                async for replies in get_new_chunk_iter(youtube_fetch_childrens(parent_id), 2):
                    await push_to_neo4j(replies)
            except InputError:
                update = datetime.now()+TIMEDELTA_WRONG_DATA_UPDATE
            async with pool.acquire() as con:
                await con.execute(queries.updated_insert_query, parent_id, update)
    return f


@run_in_executor
def neo4j_blocking_query(neo4j: Neo4jConnection, query: str, videos: 'list[Reply]'):
    neo4j.bulk_insert_data(query, list(map(asdict, videos)))


@run_in_executor
def get_neo4j():
    return Neo4jConnection()


@run_in_executor
def close_neo4j(neo4j: Neo4jConnection):
    neo4j.close()


async def push_to_neo4j(comments: 'list[Reply]'):
    if len(comments) == 0:
        return
    log.info("neo4j will save %d comments", len(comments))
    neo4j = await get_neo4j()  # type: ignore
    await neo4j_blocking_query(neo4j, queries.all_comment_query,  # type: ignore
                               comments)
    await close_neo4j(neo4j)  # type: ignore


def parse_messages(message):
    key = message.key.decode("utf-8")
    value = pickle.loads(message.value)
    return ParentMessage(key, value[0], list(map(from_dict, value[1])))


async def timeout_to_quota_reset():
    now = datetime.now()
    same_day_update = datetime(
        year=now.year, month=now.month, day=now.day, hour=10)
    next_day_update = datetime(
        year=now.year, month=now.month, day=now.day, hour=10)+timedelta(days=1)
    next_update = same_day_update if now.hour < 10 else next_day_update
    delta = (datetime.now()-next_update)
    log.warning("youtube fetch failed and is waiting for %s", delta)
    await asyncio.sleep(delta.total_seconds())


async def youtube_fetch_childrens(parent_id: CommentId):
    async with Aiogoogle(api_key=DEVELOPER_KEY) as aiogoogle:
        youtube_api = await aiogoogle.discover('youtube', 'v3')
        req = youtube_api.comments.list(
            part="snippet,id", parentId=parent_id, maxResults=YOUTUBE_COMMENTS_THREAD_CHUNK)  # type: ignore
        while True:
            try:
                full_res = await aiogoogle.as_api_key(req, full_res=True)
                page_number = 0
                async for page in full_res:
                    rejected = []
                    parsed: list[Reply] = []
                    for item in page['items']:
                        try:
                            parsed.append(from_json(item))
                        except KeyError:
                            rejected.append(item)
                    if len(rejected) > 0:
                        with open(f'./rejected/replies-{datetime.now()}.json', 'w') as f:
                            json.dump(rejected, f)
                    yield parsed
                    page_number += 1
                # precisely (page_number-1)*YOUTUBE_COMMENTS_THREAD_CHUNK + len(rejected) + len(parsed)
                log.debug("Fetched from youtube around %d comments",
                          page_number*YOUTUBE_COMMENTS_THREAD_CHUNK)
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
    log.info("update frequency: %d", data.update_frequency)
    replyConsumer = AIOKafkaConsumer(
        'comment_replies',
        bootstrap_servers='kafka:9092',
        enable_auto_commit=False,      # Will disable autocommit
        auto_offset_reset="earliest",  # If committed offset not found, start from beginning
        group_id="replyTest3Module"
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
