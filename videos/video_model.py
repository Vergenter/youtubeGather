from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Optional

from isodate.duration import Duration
from utils.types import VideoId, ChannelId
import isodate


def wrapped_parse_timeDelta(datestr: str) -> timedelta:
    parsed = isodate.parse_duration(datestr)
    if isinstance(parsed, timedelta):
        return parsed
    raise TypeError("Duration gives wrong type")


@dataclass
class Video:
    video_id: VideoId
    publishedAt: datetime
    channel_id: ChannelId
    title: str
    description: str
    tags: 'list[str]'  # custom user tags
    category_id: str  # single category id
    liveBroadcastContent: bool  # live/none/upcoming -> none<=>false
    defaultLanguage: Optional[str]  # language
    defaultAudioLanguage: Optional[str]  # language
    duration: timedelta  # ISO 8601 duration
    is3D: bool  # dimension =="3D"
    ishd: bool  # if definition==hd
    hasCaption: bool
    licensedContent: bool
    regionRestrictionAllowed: 'list[str]'  # regions
    regionRestrictionBlocked: 'list[str]'  # regions
    is360degree: bool  # projection =="360"
    status: str  # uploadStatus + failureReason + rejectionReason
    public: bool  # privacyStatus == "public"
    cclicense: bool  # license = "creativeCommon"
    embeddable: bool
    publicStatsViewable: bool
    madeForKids: bool
    viewCount: int
    likeCount: int
    dislikeCount: int
    commentCount: int
    topicCategories: 'list[str]'  # urls
    localizations: 'list[str]'  # languages
    update: datetime = datetime.now()


def from_json(json: Any):
    """throws KeyError"""
    return Video(
        video_id=VideoId(json["id"]),
        publishedAt=json["snippet"]["publishedAt"],
        channel_id=json["snippet"]["channelId"],
        title=json["snippet"]["title"],
        description=json["snippet"]["description"],
        tags=json["snippet"].get("tags", []),
        category_id=json["snippet"]["categoryId"],
        liveBroadcastContent=json["snippet"]["categoryId"] != "none",
        defaultLanguage=json["snippet"].get("defaultLanguage"),
        defaultAudioLanguage=json["snippet"].get("defaultAudioLanguage"),
        duration=wrapped_parse_timeDelta(json["contentDetails"]["duration"]),
        is3D=json["contentDetails"]["dimension"] == "3d",
        ishd=json["contentDetails"]["definition"] == "hd",
        hasCaption=json["contentDetails"]["caption"],
        licensedContent=json["contentDetails"]["licensedContent"],
        regionRestrictionAllowed=json.get(
            "regionRestriction", {}).get("allowed", []),
        regionRestrictionBlocked=json.get(
            "regionRestriction", {}).get("blocked", []),
        is360degree=json["contentDetails"]["projection"] == "360",
        status=json["status"].get("failureReason") or json["status"].get(
            "rejectionReason") or json["status"]["uploadStatus"],
        public=json["status"]["privacyStatus"] == "public",
        cclicense=json["status"]["license"] == "creativeCommon",
        embeddable=json["status"]["embeddable"],
        publicStatsViewable=json["status"]["publicStatsViewable"],
        madeForKids=json["status"]["madeForKids"],
        viewCount=json["statistics"]["viewCount"],
        likeCount=json["statistics"].get("likeCount"),
        dislikeCount=json["statistics"].get("dislikeCount"),
        commentCount=json["statistics"].get("commentCount"),
        topicCategories=json.get("topicDetails", {}).get(
            "topicCategories", []),
        localizations=list(json.get("localizations", []))
    )
