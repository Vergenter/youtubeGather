from typing import Any
from channels.channels_config import ChannelsConfig, ChannelsModuleConfig, ChannelsUpdateConfig, IncludeConfig


def channels_update_config_parser(config: Any):
    return [ChannelsUpdateConfig(channel_id, config.get(channel_id, ChannelsUpdateConfig.update_frequency)) for channel_id in config]


def channels_config_parser(channels: Any):
    return ChannelsConfig(
        update=(update := channels.get("update")) and channels_update_config_parser(
            update) or ChannelsConfig.update,
        update_frequency=channels.get(
            "update_frequency", ChannelsConfig.update_frequency)
    )


def include_config_parser(include: Any):
    return IncludeConfig(
        channels=include.get("channels", IncludeConfig.channels)
    )


def channels_module_config_parser(config: Any):
    return ChannelsModuleConfig(
        channelsConfig=(channels := config.get(
            "channels")) and channels_config_parser(channels) or ChannelsModuleConfig.channelsConfig,
        includeConfig=(include := config.get("include")
                       ) and include_config_parser(include) or ChannelsModuleConfig.includeConfig
    )
