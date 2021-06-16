from datetime import datetime
from py2neo.ogm import Model, Property, RelatedFrom, RelatedTo
from models.Video import Video_stub
from models.Channel import Channel


class Comment(Model):
    __primarylabel__ = "Comment"
    __primarykey__ = "commentId"

    commentId = Property()
    etag = Property()
    publishedAt = Property()
    updatedAt = Property()
    likeCount = Property()
    totalReplyCount = Property()
    textOriginal = Property()
    parent = RelatedFrom("Comment", "TO")
    child = RelatedTo("Comment", "TO")
    video = RelatedTo(Video_stub, "TO")
    channel = RelatedTo(Channel, "OWNED_BY")

    def __init__(self, commentId: str, etag: str, publishedAt: datetime,
                 updatedAt: datetime, likeCount: int, totalReplyCount, textOriginal: str):
        self.commentId = commentId
        self.etag = etag
        self.publishedAt = publishedAt
        self.updatedAt = updatedAt
        self.likeCount = likeCount
        self.totalReplyCount = totalReplyCount
        self.textOriginal = textOriginal


def getComment(json, replyCount=0):
    """Create Comment model from json"""
    channel = Channel(json["snippet"]["authorChannelId"]["value"])
    video = Video_stub(json["snippet"]["videoId"])
    comment = Comment(
        commentId=json["id"],
        etag=json["etag"],
        publishedAt=datetime.strptime(json["snippet"]["publishedAt"],
                                      '%Y-%m-%dT%H:%M:%SZ'),
        updatedAt=datetime.strptime(json["snippet"]["updatedAt"],
                                    '%Y-%m-%dT%H:%M:%SZ'),
        likeCount=int(json["snippet"]["likeCount"]),
        totalReplyCount=int(replyCount),
        textOriginal=json["snippet"]["textOriginal"],
    )

    comment.channel.add(channel)
    comment.video.add(video)
    return comment


def fromJson(json):
    """Create Comment model with relationships from json"""
    topLevelComment = getComment(
        json["snippet"]["topLevelComment"], json["snippet"]["totalReplyCount"])
    if json.get("replies") is not None:
        for comment in json["replies"]["comments"]:
            topLevelComment.child.add(getComment(comment))
    return topLevelComment
