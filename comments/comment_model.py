import asyncio
from typing import Any, Awaitable, Callable, Coroutine, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta

from utils.types import ChannelId, CommentId, ReplyId, VideoId


class QuotaManager:
    # dostawanie quoty -> zużywanie quoty i czekanie jak jej nie ma
    # reagowanie na update'y by ją znormalizować
    #? przesuwanie quoty w ciągu dnia pomiędzy updateami

    def __init__(self, quota: int, quota_update_value:int,quota_reset_period_h:float):
        self._quota: int = quota
        self._quota_update_value: int = quota_update_value
        self.update_attempt_period_s = timedelta(hours=quota_reset_period_h).total_seconds()
        self.updater = asyncio.create_task(self.update_loop())

    def __del__(self):
        self.updater.cancel()

    async def update_loop(self):
        while True:
            self.update_taks = asyncio.create_task(self.update_quota())
            await self.update_taks

    async def update_quota(self):
        self._quota = self._quota_update_value
        await asyncio.sleep(self.update_attempt_period_s)

    async def get_quota(self, quota_usage: int):
        while True:
            if self._quota >= quota_usage:
                self._quota -= quota_usage
                break
            else:
                await self.update_taks

    def quota_exceeded(self):
        self._quota = 0


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
        authorChannelId=(channel_id := snippet["topLevelComment"]["snippet"].get(
            "authorChannelId", {}).get("value")) and ChannelId(channel_id),
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
        authorChannelId=(channel_id := reply["snippet"].get(
            "authorChannelId", {}).get("value")) and ChannelId(channel_id),
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
