from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class RepliesConfig:
    update_frequency: int = 0


def from_yaml(config):
    return RepliesConfig(
        update_frequency=config.get(
            "update_frequency", RepliesConfig.update_frequency)
    )
