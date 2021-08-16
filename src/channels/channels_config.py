from dataclasses import dataclass
from typing import Iterable

from utils.types import ChannelId


@dataclass(frozen=True)
class ChannelsUpdateConfig:
    channel_id: ChannelId
    update_frequency: int = 0


@dataclass(frozen=True)
class ChannelsConfig:
    update: 'Iterable[ChannelsUpdateConfig]' = ()
    update_frequency: int = 0


@dataclass(frozen=True)
class IncludeConfig:
    channels: 'Iterable[ChannelId]' = ()


@dataclass(frozen=True)
class ChannelsModuleConfig:
    includeConfig: IncludeConfig = IncludeConfig()
    channelsConfig: ChannelsConfig = ChannelsConfig()
