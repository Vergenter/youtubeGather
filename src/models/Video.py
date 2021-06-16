""" Define Models"""
# pylint: disable=C0103”
from datetime import datetime
from py2neo.ogm import Model, Property, RelatedFrom, RelatedTo
from models.Game import Game
from models.Channel import Channel


class Video_stub(Model):
    """Video object model"""
    __primarylabel__ = "Video"
    __primarykey__ = "videoId"
    comment = RelatedFrom("Comment", "TO")
    videoId = Property()

    def __init__(self, videoId: str):
        self.videoId = videoId


class Video(Model):
    """Video object model"""
    __primarylabel__ = "Video"
    __primarykey__ = "videoId"
    videoId = Property()
    etag = Property()
    publishedAt = Property()
    publishTime = Property()
    title = Property()
    description = Property()
    liveBroadcastContent = Property()
    game = RelatedTo(Game, "OF_GAME")
    channel = RelatedTo(Channel, "OWNED_BY")

    def __init__(self, videoId: str, etag: str, publishedAt: datetime,
                 publishTime: datetime, title: str, description: str, liveBroadcastContent: bool):
        self.videoId = videoId
        self.etag = etag
        self.publishedAt = publishedAt
        self.publishTime = publishTime
        self.title = title
        self.description = description
        self.liveBroadcastContent = liveBroadcastContent


def fromJson(json):
    """Create Video model with relationships from json"""
    channel = Channel(channelId=json["snippet"]["channelId"])
    game = Game(title=json["gameTitle"], year=json["gameSubtitle"])
    video = Video(
        videoId=json["id"]["videoId"],
        etag=json["etag"],
        publishedAt=datetime.strptime(json["snippet"]["publishedAt"],
                                      '%Y-%m-%dT%H:%M:%SZ'),
        publishTime=datetime.strptime(json["snippet"]["publishTime"],
                                      '%Y-%m-%dT%H:%M:%SZ'),
        title=json["snippet"]["title"],
        description=json["snippet"]["description"],
        liveBroadcastContent=json["snippet"]["liveBroadcastContent"] != "none",
    )
    if game.title is not None:
        video.game.add(game)
    video.channel.add(channel)
    return video
