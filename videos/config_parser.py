from typing import Any

from videos.videos_config import ChannelVideosConfig, ChannelVideosUpdateConfig, IncludeConfig, VideosConfig, VideosUpdateConfig, VideosModuleConfig


def videos_update_config_parser(config: Any):
    return [VideosUpdateConfig(video_id, config.get(video_id, VideosUpdateConfig.update_frequency)) for video_id in config]


def videos_config_parser(videos: Any):
    return VideosConfig(
        update=(update := videos.get("update")) and videos_update_config_parser(
            update) or VideosConfig.update,
        update_frequency=videos.get(
            "update_frequency", VideosConfig.update_frequency)
    )


def channel_Videos_update_config_parser(config: Any):
    return [ChannelVideosUpdateConfig(video_id, config.get(video_id, ChannelVideosUpdateConfig.update_frequency)) for video_id in config]


def channel_Videos_config_parser(channels: Any):
    return ChannelVideosConfig(
        update=(update := channels.get("update")) and channel_Videos_update_config_parser(
            update) or ChannelVideosConfig.update,
        update_frequency=channels.get(
            "update_frequency", ChannelVideosConfig.update_frequency)
    )


def include_config_parser(include: Any):
    return IncludeConfig(
        videos=include.get("videos", IncludeConfig.videos),
        channels_videos=include.get(
            "channels_videos", IncludeConfig.channels_videos)
    )


def videos_module_config_parser(config: Any):
    return VideosModuleConfig(
        channelVideosConfig=(channel_videos := config.get(
            "channel_videos")) and channel_Videos_config_parser(channel_videos) or VideosModuleConfig.channelVideosConfig,
        videosConfig=(videos := config.get("videos")) and videos_config_parser(
            videos) or VideosModuleConfig.videosConfig,
        includeConfig=(include := config.get("include")
                       ) and include_config_parser(include) or VideosModuleConfig.includeConfig
    )
