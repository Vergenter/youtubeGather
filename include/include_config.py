from dataclasses import dataclass, field


@dataclass(frozen=True)
class include:
    videos: 'list[str]' = field(default_factory=list)
    channels: 'list[str]' = field(default_factory=list)
