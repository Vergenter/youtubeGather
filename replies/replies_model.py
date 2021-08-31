from dataclasses import dataclass
from datetime import datetime
from utils.types import ChannelId, CommentId, ReplyId, VideoId


@dataclass
class Reply:
    reply_id: ReplyId
    textOriginal: str
    parentId: CommentId
    authorDisplayName: str
    authorChannelId: ChannelId
    likeCount: int
    publishedAt: datetime
    updatedAt: datetime
    update: datetime


@dataclass
class ParentMessage:
    parent_id: CommentId
    total_replies: int
    replies: list[Reply]


def from_dict(data: dict):
    return Reply(
        reply_id=ReplyId(data["reply_id"]),
        textOriginal=data["textOriginal"],
        parentId=CommentId(data["parentId"]),
        authorDisplayName=data["authorDisplayName"],
        authorChannelId=ChannelId(data["authorChannelId"]),
        likeCount=data["likeCount"],
        publishedAt=data["publishedAt"],
        updatedAt=data["updatedAt"],
        update=data["update"]
    )


def from_json(json):
    return Reply(
        reply_id=ReplyId(json["id"]),
        textOriginal=json["snippet"]["textOriginal"],
        parentId=CommentId(json["snippet"]["parentId"]),
        authorDisplayName=json["snippet"]["authorDisplayName"],
        authorChannelId=ChannelId(json["snippet"]["authorChannelId"]["value"]),
        likeCount=json["snippet"]["likeCount"],
        publishedAt=json["snippet"]["publishedAt"],
        updatedAt=json["snippet"]["updatedAt"],
        update=datetime.now()
    )
