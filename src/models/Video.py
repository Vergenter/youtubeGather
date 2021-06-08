from py2neo.ogm import Model, Property, RelatedFrom, RelatedTo
from datetime import datetime


class Game(Model):
    __primarylabel__ = "Game"
    __primarykey__ = "title"
    title = Property()
    year = Property()
    videos = RelatedTo("Video", "OF_GAME")

    def __init__(self, title: str, year: int):
        self.title = title
        self.year = year


class Channel(Model):
    __primarylabel__ = "Channel"
    __primarykey__ = "channelId"
    channelId = Property()
    videos = RelatedTo("Video", "OWNED_BY")

    def __init__(self, channelId: str):
        self.channelId = channelId


class Video(Model):
    __primarylabel__ = "Video"
    __primarykey__ = "videoId"
    videoId = Property()
    etag = Property()
    publishedAt = Property()
    publishTime = Property()
    title = Property()
    description = Property()
    liveBroadcastContent = Property()
    game = RelatedFrom(Game, "OF_GAME")
    channel = RelatedFrom(Channel, "OWNED_BY")

    def __init__(self, videoId: str, etag: str, publishedAt: datetime, publishTime: datetime, title: str, description: str, liveBroadcastContent: bool):
        self.videoId = videoId
        self.etag = etag
        self.publishedAt = publishedAt
        self.publishTime = publishTime
        self.title = title
        self.description = description
        self.liveBroadcastContent = liveBroadcastContent


def fromJson(json):
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
    if(game.title is not None):
        video.game.add(game)
    video.channel.add(channel)
    return video
