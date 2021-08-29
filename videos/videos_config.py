from dataclasses import dataclass


@dataclass(frozen=True)
class VideosConfig:
    update_frequency: int = 0


def from_yaml(config):
    return VideosConfig(update_frequency=config.get("update_frequency", 0))
