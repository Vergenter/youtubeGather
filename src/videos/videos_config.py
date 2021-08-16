from dataclasses import dataclass, field
from typing import Iterable
from utils.types import ChannelId, VideoId


@dataclass(frozen=True)
class ChannelVideosUpdateConfig:
    channelId: ChannelId
    update_frequency: int = 0


@dataclass(frozen=True)
class ChannelVideosConfig:
    update: 'Iterable[ChannelVideosUpdateConfig]' = ()
    update_frequency: int = 0


@dataclass(frozen=True)
class VideosUpdateConfig:
    video_id: VideoId
    update_frequency: int = 0


@dataclass(frozen=True)
class VideosConfig:
    update: 'Iterable[VideosUpdateConfig]' = ()
    update_frequency: int = 0


@dataclass(frozen=True)
class IncludeConfig:
    channels_videos: 'Iterable[ChannelId]' = ()
    videos: 'Iterable[VideoId]' = ()


@dataclass(frozen=True)
class VideosModuleConfig:
    channelVideosConfig: ChannelVideosConfig = ChannelVideosConfig()
    videosConfig: VideosConfig = VideosConfig()
    includeConfig: IncludeConfig = IncludeConfig()
