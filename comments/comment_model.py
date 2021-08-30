from dataclasses import dataclass
from datetime import datetime
from utils.types import ChannelId, CommentId, ReplyId, VideoId


@dataclass
class Comment:
    comment_id: CommentId
    video_id: VideoId
    textOriginal: str
    authorDisplayName: str
    authorChannelId: ChannelId
    likeCount: int
    publishedAt: datetime
    updatedAt: datetime
    totalReplyCount: int
    isPublic: bool
    update: datetime


def comment_from_json(snippet):
    return Comment(
        comment_id=CommentId(snippet["topLevelComment"]["id"]),
        video_id=VideoId(snippet["videoId"]),
        textOriginal=snippet["topLevelComment"]["snippet"]["textOriginal"],
        authorDisplayName=snippet["topLevelComment"]["snippet"]["authorDisplayName"],
        authorChannelId=ChannelId(
            snippet["topLevelComment"]["snippet"]["authorChannelId"]["value"]),
        likeCount=snippet["topLevelComment"]["snippet"]["likeCount"],
        publishedAt=snippet["topLevelComment"]["snippet"]["publishedAt"],
        updatedAt=snippet["topLevelComment"]["snippet"]["updatedAt"],
        totalReplyCount=snippet["totalReplyCount"],
        isPublic=snippet["isPublic"],
        update=datetime.now()
    )


@dataclass
class Reply:
    reply_id: ReplyId
    video_id: VideoId
    textOriginal: str
    parentId: CommentId
    authorDisplayName: str
    authorChannelId: ChannelId
    likeCount: int
    publishedAt: datetime
    updatedAt: datetime
    update: datetime


def reply_from_json(reply):
    return Reply(
        reply_id=ReplyId(reply["id"]),
        video_id=VideoId(reply["snippet"]["videoId"]),
        textOriginal=reply["snippet"]["textOriginal"],
        parentId=CommentId(reply["snippet"]["parentId"]),
        authorDisplayName=reply["snippet"]["authorDisplayName"],
        authorChannelId=ChannelId(
            reply["snippet"]["authorChannelId"]["value"]),
        likeCount=reply["snippet"]["likeCount"],
        publishedAt=reply["snippet"]["publishedAt"],
        updatedAt=reply["snippet"]["updatedAt"],
        update=datetime.now()
    )


@dataclass
class CommentThread:
    top_level_comment: Comment
    replies: 'list[Reply]'


def from_json(json):
    return CommentThread(
        comment_from_json(json["snippet"]),
        list(map(reply_from_json, json.get("replies", {}).get("comments", [])))
    )
