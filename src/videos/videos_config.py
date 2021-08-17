from dataclasses import dataclass
from typing import Sequence
from utils.types import ChannelId, VideoId


@dataclass(frozen=True)
class ChannelVideosUpdateConfig:
    channelId: ChannelId
    update_frequency: int = 0


@dataclass(frozen=True)
class ChannelVideosConfig:
    update: 'Sequence[ChannelVideosUpdateConfig]' = ()
    update_frequency: int = 0


@dataclass(frozen=True)
class VideosUpdateConfig:
    video_id: VideoId
    update_frequency: int = 0


@dataclass(frozen=True)
class VideosConfig:
    update: 'Sequence[VideosUpdateConfig]' = ()
    update_frequency: int = 0


@dataclass(frozen=True)
class IncludeConfig:
    channels_videos: 'Sequence[ChannelId]' = ()
    videos: 'Sequence[VideoId]' = ()


@dataclass(frozen=True)
class VideosModuleConfig:
    channelVideosConfig: ChannelVideosConfig = ChannelVideosConfig()
    videosConfig: VideosConfig = VideosConfig()
    includeConfig: IncludeConfig = IncludeConfig()
