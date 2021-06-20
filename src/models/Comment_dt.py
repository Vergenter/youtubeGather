from typing import NamedTuple
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Optional, Callable, TypeVar

T = TypeVar('T')
Y = TypeVar('Y')


def map_optional(mapper: Callable[[T], Y], optional: Optional[T]) -> Optional[Y]:
    if T is None:
        return None
    return mapper(optional)


@dataclass(frozen=True)
class Comment_parent_dc:
    commentId: str


@dataclass(frozen=True)
class Comment_dc(Comment_parent_dc):
    etag: str
    publishedAt: datetime
    updatedAt: datetime
    likeCount: int
    totalReplyCount: int
    textOriginal: str

    def __hash__(self):
        return self.commentId


@dataclass(frozen=True)
class Channel_dc:
    channelId: str

    def __hash__(self):
        return self.channelId


@dataclass(frozen=True)
class Video_dc:
    videoId: str

    def __hash__(self):
        return self.videoId


@dataclass(frozen=True)
class Comment_row_dc:
    comment: Comment_dc
    channel: Channel_dc
    video: Video_dc
    parentId: Optional[str]


def getComment_dc(json, replyCount=0):
    return Comment_dc(
        commentId=json["id"],
        etag=json["etag"],
        publishedAt=datetime.strptime(json["snippet"]["publishedAt"],
                                      '%Y-%m-%dT%H:%M:%SZ'),
        updatedAt=datetime.strptime(json["snippet"]["updatedAt"],
                                    '%Y-%m-%dT%H:%M:%SZ'),
        likeCount=int(json["snippet"]["likeCount"]),
        textOriginal=json["snippet"]["textOriginal"],
        totalReplyCount=int(replyCount),
    )


def get_Comment_row_dc(json, replyCount=0, parentId: Optional[str] = None):
    return Comment_row_dc(
        getComment_dc(json, replyCount),
        Channel_dc(json["snippet"]["authorChannelId"]["value"]),
        Video_dc(json["snippet"]["videoId"]),
        parentId
    )


def fromJson_dc(json):
    """Create Comment model with relationships from json"""
    topLevelComment = get_Comment_row_dc(
        json["snippet"]["topLevelComment"], replyCount=json["snippet"].get("totalReplyCount", 0))
    bottomLevelComments = [get_Comment_row_dc(comment, parentId=topLevelComment.comment.commentId)
                           for comment in json.get("replies", {"comments": []})["comments"]]
    return list(map(lambda x: asdict(x), [topLevelComment, *bottomLevelComments]))


def get_Comment_child_row_dc(json, parentId: Optional[str] = None):
    return Comment_row_dc(
        getComment_dc(json),
        Channel_dc(json["snippet"]["authorChannelId"]["value"]),
        Video_dc(json["snippet"]["videoId"]),
        parentId
    )


def fromJson_child_dc(json):
    return asdict(get_Comment_child_row_dc(json, parentId=json["snippet"]["parentId"]))


comment_query = '''
UNWIND $rows AS row
MATCH (video:Video{videoId: row.video.videoId})
CREATE (comment:Comment{
    commentId: row.comment.commentId,
    etag: row.comment.etag,
    publishedAt: row.comment.publishedAt,
    updatedAt: row.comment.updatedAt,
    likeCount: row.comment.likeCount,
    totalReplyCount: row.comment.totalReplyCount,
    textOriginal: row.comment.textOriginal}
)
MERGE (channel:Channel{channelId: row.channel.channelId})
CREATE (comment)-[:OWNED_BY]->(channel)
CREATE (comment)-[:TO]->(video)

WITH comment,row
MATCH (parent:Comment{commentId: row.parentId})
CREATE (comment)-[:TO]->(parent)
'''

comment_2_query = '''
UNWIND $rows AS row
MATCH (video:Video{videoId: row.video.videoId})
MERGE (comment:Comment{
    commentId: row.comment.commentId,
    etag: row.comment.etag,
    publishedAt: row.comment.publishedAt,
    updatedAt: row.comment.updatedAt,
    likeCount: row.comment.likeCount,
    totalReplyCount: row.comment.totalReplyCount,
    textOriginal: row.comment.textOriginal}
)
MERGE (channel:Channel{channelId: row.channel.channelId})
MERGE (comment)-[:OWNED_BY]->(channel)
MERGE (comment)-[:TO]->(video)

WITH comment,row
MATCH (parent:Comment{commentId: row.parentId})
MERGE (comment)-[:TO]->(parent)
'''
