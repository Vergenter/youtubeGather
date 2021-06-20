from datetime import datetime
from py2neo.ogm import Model, Property, RelatedFrom, RelatedTo

from dataclasses import dataclass
from neo4j.time import DateTime
from typing import Optional


@dataclass
class Channel_stub_dc:
    channelId: str


class Channel(Model):
    """Channel id object model"""
    __primarylabel__ = "Channel"
    __primarykey__ = "channelId"
    channelId = Property()
    comment = RelatedFrom("Comment", "OWNED_BY")
    videos = RelatedFrom("Video", "OWNED_BY")

    def __init__(self, channelId: str):
        self.channelId = channelId
