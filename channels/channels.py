import logging
from dataclasses import asdict
import json

from aiogoogle.excs import HTTPError
from utils.connection import Neo4jConnection
from utils.sync_to_async import run_in_executor
from channel_model import Channel, from_json
from datetime import datetime, timedelta
import queries
from aiogoogle.client import Aiogoogle
from utils.chunk_gen import get_new_chunk_queue
from kafka.structs import TopicPartition
from channels_config import ChannelsConfig, from_yaml
import yaml
from aiokafka import AIOKafkaConsumer
import asyncio
from utils.types import ChannelId
import asyncpg
import os
from operator import attrgetter

LOGGER_NAME = "CHANNELS"
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
log = logging.getLogger(LOGGER_NAME)

TIMEDELTA_WRONG_DATA_UPDATE = timedelta(weeks=52*1000)
DEVELOPER_KEY = os.environ['YOUTUBE_API_KEY_V3']
YOUTUBE_CHANNELS_MAX_CHUNK = 50
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


async def process_filter(consumer: AIOKafkaConsumer, pool: asyncpg.Pool, inQueue: asyncio.Queue, outQueue: asyncio.Queue, new_channels: 'set[ChannelId]'):
    """new_channels parameter to provide new channel only once to outQueue"""
    kafka_commit = True
    async for messages in get_new_chunk_queue(inQueue, 5, 1000):
        if len(messages) > 0:
            async with pool.acquire() as con:
                # need to parse args to ids
                values = {ChannelId(i["id"]) for i in await con.fetch(queries.new_channel_query, list(map(ChannelId, map(attrgetter("value"), messages))))}
                if kafka_commit:
                    kafka_commit = await kafka_commit_from_messages(consumer, values, messages)
                log.debug("From kafka got %d new channels", len(values))
                for value in values:
                    if value not in new_channels:
                        new_channels.add(value)
                        await outQueue.put(value)


async def process_update(pool: asyncpg.Pool, frequency: int, inQueue: asyncio.Queue, outQueue: asyncio.Queue):
    log.info("Update triggered")
    if frequency > 0:
        async for updates in get_new_chunk_queue(inQueue, 5):
            if len(updates) > 0:
                async with pool.acquire() as con:
                    values = await con.fetch(queries.channel_update_query, datetime.now() - timedelta(days=frequency))
                    for value in values:
                        await outQueue.put(value)


async def fetch_channels_from_yt(channels_ids: 'list[ChannelId]'):
    async with Aiogoogle(api_key=DEVELOPER_KEY) as aiogoogle:
        youtube_api = await aiogoogle.discover('youtube', 'v3')
        req = youtube_api.channels.list(
            part="brandingSettings,contentDetails,contentOwnerDetails,id,localizations,snippet,statistics,status,topicDetails", id=",".join(channels_ids), maxResults=YOUTUBE_CHANNELS_MAX_CHUNK, hl="en_US")  # type: ignore
        parsed: list[Channel] = []
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
                    "youtube fetch failed %s and is waiting for %s", err.res, delta)
                await asyncio.sleep(delta.total_seconds())

        rejected = []
        for item in result["items"]:
            try:
                parsed.append(from_json(item))
            except (KeyError, ValueError):
                rejected.append(item)
        if len(rejected) > 0:
            with open(f'./rejected/channels-{datetime.now()}.json', 'w') as f:
                json.dump(rejected, f)
        return parsed


@run_in_executor
def neo4j_blocking_query(neo4j: Neo4jConnection, query: str, channels: 'list[Channel]'):
    neo4j.bulk_insert_data(query, list(map(asdict, channels)))


@run_in_executor
def get_neo4j():
    return Neo4jConnection()


@run_in_executor
def close_neo4j(neo4j: Neo4jConnection):
    neo4j.close()


async def push_to_neo4j(channels: 'list[Channel]', new_channels: 'set[ChannelId]'):
    if len(channels) == 0:
        return
    channels_to_create = [
        channel for channel in channels if channel.channel_id in new_channels]
    neo4j = await get_neo4j()
    log.info("neo4j will save %d channels", len(channels))
    if len(channels_to_create) > 0:
        await asyncio.gather(neo4j_blocking_query(neo4j, queries.static_channel_query, channels_to_create),
                             neo4j_blocking_query(neo4j, queries.dynamic_channel_query, channels_to_create))
    else:
        await neo4j_blocking_query(
            neo4j, queries.dynamic_channel_query, channels_to_create)

    await close_neo4j(neo4j)


async def insert_update(pool: asyncpg.Pool, channels: 'list[Channel]', potentialy_wrong_channels_ids: 'list[ChannelId]'):
    if len(channels) == 0 and len(potentialy_wrong_channels_ids) == 0:
        return
    channels_to_update = list(map(queries.channel_to_update, channels))
    if len(channels) != len(potentialy_wrong_channels_ids):
        error_update_time = datetime.now() + TIMEDELTA_WRONG_DATA_UPDATE
        channels_to_update.extend(map(queries.to_update(error_update_time), {
                                  i.channel_id for i in channels}.symmetric_difference(potentialy_wrong_channels_ids)))
        log.info("number of errous channel_id %d", len(
            potentialy_wrong_channels_ids)-len(channels))
    async with pool.acquire() as con:
        await con.executemany(queries.update_insert_query, channels_to_update)
        log.debug("inserted to postgres %d updated", len(channels))


async def main(data: ChannelsConfig):
    log.info("update frequency: %d", data.update_frequency)
    channelConsumer = AIOKafkaConsumer(
        'new_channels',
        bootstrap_servers='kafka:9092',
        enable_auto_commit=False,      # Will disable autocommit
        auto_offset_reset="earliest",  # If committed offset not found, start from beginning
        group_id="channelModule"
    )
    updateConsumer = AIOKafkaConsumer(
        'updates',
        bootstrap_servers='kafka:9092',
        enable_auto_commit=True,
        auto_offset_reset="latest",
    )
    postgres_pool = asyncpg.create_pool(
        user=user, password=password, database=dbname, host=host, command_timeout=5)
    _, _, pool = await asyncio.gather(channelConsumer.start(), updateConsumer.start(), postgres_pool)
    if not pool:
        raise NotImplementedError("no connctions")
    try:
        input_channel = asyncio.Queue()
        update_channel = asyncio.Queue()
        processing_channel = asyncio.Queue()
        new_channels: 'set[ChannelId]' = set()
        asyncio.create_task(kafka_produce(channelConsumer, input_channel))
        asyncio.create_task(kafka_produce(updateConsumer, update_channel))
        asyncio.create_task(process_filter(channelConsumer,
                                           pool, input_channel, processing_channel, new_channels))
        asyncio.create_task(process_update(
            pool, data.update_frequency, update_channel, processing_channel))
        # long waiting chunk for youtube
        # add all items to set
        async for channels_ids in get_new_chunk_queue(processing_channel, 30, YOUTUBE_CHANNELS_MAX_CHUNK):
            # timeout even for one day
            log.debug("fetching data from youtube")
            channels = await fetch_channels_from_yt(channels_ids)
            await push_to_neo4j(channels, new_channels)
            await insert_update(pool, channels, channels_ids)
            new_channels.difference_update(channels_ids)

    finally:
        await asyncio.gather(channelConsumer.stop(), updateConsumer.stop(), pool.close())


if __name__ == "__main__":
    log.info("service started")
    with open("config.yaml", 'r', encoding='utf-8') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        asyncio.run(main(from_yaml(config)))
