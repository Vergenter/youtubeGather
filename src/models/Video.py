""" Define Models"""
# pylint: disable=C0103”
from datetime import datetime
from py2neo.ogm import Model, Property, RelatedFrom, RelatedTo
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class Channel_dc:
    channelId: str

    def __hash__(self):
        return self.channelId


@dataclass(frozen=True)
class Video_dc:
    videoId: str
    etag: str
    publishedAt: datetime
    publishTime: datetime
    title: str
    description: str
    liveBroadcastContent: bool

    def __hash__(self):
        return self.videoId


@dataclass(frozen=True)
class Game_dc:
    title: str
    year: int

    def __hash__(self):
        return self.title


@dataclass(frozen=True)
class Video_row_dc:
    video: Video_dc
    channel: Channel_dc
    game: Game_dc


def fromJson(json):
    """Create Video model with relationships from json"""
    channel = Channel_dc(channelId=json["snippet"]["channelId"])
    game = Game_dc(title=json["gameTitle"], year=json["gameSubtitle"])
    video = Video_dc(
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
    return asdict(Video_row_dc(video, channel, game))


video_query = '''
UNWIND $rows AS row
MERGE (channel:Channel{channelId: row.channel.channelId})
MERGE (game:Game{title: row.game.title})
CREATE (video:Video{
    videoId: row.video.videoId,
    etag: row.video.etag,
    publishedAt: row.video.publishedAt,
    publishTime: row.video.publishTime,
    title: row.video.title,
    description: row.video.description,
    liveBroadcastContent: row.video.liveBroadcastContent}
)
CREATE (video)-[:OWNED_BY]->(channel)
CREATE (video)-[:OF_GAME]->(game)
'''
