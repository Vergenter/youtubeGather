from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    data_update_period_d: float
    update_attempt_period_h: int
    quota_per_attempt_limit: int


def from_yaml(config):
    return Config(
        data_update_period_d=config.get("data_update_period_d", 0),
        update_attempt_period_h=config.get("update_attempt_period_h", 24),
        quota_per_attempt_limit=config.get("quota_per_attempt_limit", 0)
    )
