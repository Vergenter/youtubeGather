from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional
from dataclasses import dataclass
from utils.types import ChannelId, PlaylistId

import re


def quoted_split(s):
    def strip_quotes(s):
        if s and (s[0] == '"' or s[0] == "'") and s[0] == s[-1]:
            return s[1:-1]
        return s
    return [strip_quotes(p).replace('\\"', '"').replace("\\'", "'")
            for p in re.findall(r'(?:[^"\s]*"(?:\\.|[^"])*"[^"\s]*)+|(?:[^\'\s]*\'(?:\\.|[^\'])*\'[^\'\s]*)+|[^\s]+', s)]


@dataclass
class Channel:
    channel_id: ChannelId
    publishedAt: datetime
    title: str
    description: str
    uploadsPlaylist: PlaylistId
    isLinked: bool  # isLinked==true(exist)
    viewCount: int
    subscriberCount: Optional[int]
    videoCount: int
    topicCategories: 'list[str]'
    madeForKids: bool
    public: bool  # privacyStatus == "public"
    trackingAnalyticsAccountId: Optional[str]  # static because optional
    tags: 'list[str]'  # custom user keywords
    unsubscribedTrailer: Optional[str]
    country: Optional[str]  # Country label
    customUrl: bool
    showRelatedChannels: bool  # if exist
    moderateComments: bool  # if exist
    update: datetime


def from_json(json: Any):
    """throws KeyError"""
    return Channel(
        channel_id=ChannelId(json["id"]),
        publishedAt=json["snippet"]["publishedAt"],
        title=json["snippet"]["title"],
        description=json["snippet"]["description"],
        uploadsPlaylist=json["contentDetails"]["relatedPlaylists"]["uploads"],
        viewCount=json["statistics"]["viewCount"],
        subscriberCount=json["statistics"].get("subscriberCount"),
        videoCount=json["statistics"]["videoCount"],
        topicCategories=json.get("topicDetails", {}).get(
            "topicCategories", []),
        madeForKids=json["status"].get("madeForKids", False),
        public=json["status"]["privacyStatus"] == "public",
        isLinked=json["status"]["isLinked"],
        trackingAnalyticsAccountId=json["brandingSettings"]["channel"].get(
            "trackingAnalyticsAccountId"),
        tags=quoted_split(json["brandingSettings"]
                          ["channel"].get("keywords", "")),
        unsubscribedTrailer=json["brandingSettings"]["channel"].get(
            "unsubscribedTrailer"),
        country=json["brandingSettings"]["channel"].get("country"),
        customUrl=json["snippet"].get("customUrl") != None,
        showRelatedChannels=json["brandingSettings"]["channel"].get(
            "showRelatedChannels", False),
        moderateComments=json["brandingSettings"]["channel"].get(
            "moderateComments", False),
        update=datetime.now()
    )


def get_empty_channel(channelId: ChannelId):
    return {"channel_id": channelId, "public": False, "update": datetime.now()}
