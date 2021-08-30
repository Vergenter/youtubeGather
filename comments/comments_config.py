from dataclasses import dataclass


@dataclass(frozen=True)
class CommentsConfig:
    update_frequency: int = 0


def from_yaml(config):
    return CommentsConfig(
        update_frequency=config.get(
            "update_frequency", CommentsConfig.update_frequency)
    )
