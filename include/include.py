import logging
import asyncio
from dataclasses import asdict
import os
from include_config import IncludeConfig, from_yaml
import yaml
import aiokafka
LOGGER_NAME = "INCLUDE"
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
log = logging.getLogger(LOGGER_NAME)


async def main(data: IncludeConfig):
    producer = aiokafka.AIOKafkaProducer(bootstrap_servers='kafka:9092')
    await producer.start()
    send_channels, send_videos = 0, 0
    try:
        for channel in data.channels:
            msg = str.encode(channel)
            await producer.send_and_wait("new_channels", value=msg, key=msg)
            send_channels += 1
            if send_channels % 10 == 0:
                log.info("send %d channels", send_channels)

        for video in data.videos:
            msg = str.encode(video)
            await producer.send_and_wait("new_videos", value=msg, key=msg)
            send_videos += 1
            if send_videos % 10 == 0:
                log.info("send %d videos", send_videos)
    finally:
        await producer.stop()
        log.info("send %d channels and %d videos", send_channels, send_videos)
        with open("config.yaml", 'w', encoding='utf-8') as f:
            dumped = yaml.dump(asdict(IncludeConfig(
                data.videos[send_channels:], data.channels[send_videos:])), Dumper=yaml.Dumper)
            f.write(dumped)


if __name__ == "__main__":
    print("start script")
    with open("config.yaml", 'r', encoding='utf-8') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        if not not config:
            asyncio.run(main(from_yaml(config)))
