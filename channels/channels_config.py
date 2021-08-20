from dataclasses import dataclass


@dataclass(frozen=True)
class ChannelsConfig:
    update_frequency: int = 0


def from_yaml(config):
    return ChannelsConfig(update_frequency=config.get("update_frequency", 0))
