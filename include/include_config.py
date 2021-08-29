from dataclasses import dataclass, field


@dataclass(frozen=True)
class IncludeConfig:
    videos: 'list[str]' = field(default_factory=list)
    channels: 'list[str]' = field(default_factory=list)


def from_yaml(config):
    return IncludeConfig(videos=config.get("videos", []), channels=config.get("channels", []))
