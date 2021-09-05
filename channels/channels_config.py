from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    update_frequency_h: float
    quota_per_update_limit: int


def from_yaml(config):
    return Config(
        update_frequency_h=config.get("update_frequency_h", 0),
        quota_per_update_limit=config.get("quota_per_update_limit", 0)
    )
