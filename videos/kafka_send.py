import pickle
from typing import Callable, Iterable, Tuple, TypeVar
from video_model import Video
from aiokafka.producer.producer import AIOKafkaProducer

Item = TypeVar('Item')


def parser_to_kafka(get_key: Callable[[Item], bytes], get_msg: Callable[[Item], bytes]):
    def in_parser(item: Item):
        return (get_key(item), get_msg(item))
    return in_parser


async def kafka_send(producer: AIOKafkaProducer, topic: str, key_value: Iterable[Tuple[bytes, bytes]]):
    for item in key_value:
        await producer.send_and_wait(topic, value=item[1], key=item[0])


def get_bytes_channel_id(v: Video): return str.encode(v.channel_id)


channel_parser = parser_to_kafka(get_bytes_channel_id, get_bytes_channel_id)
