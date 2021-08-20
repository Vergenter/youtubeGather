from dataclasses import dataclass
from typing import Iterable

from utils.types import CommentId


@dataclass(frozen=True)
class RepliesUpdateConfig:
    replies_id: CommentId
    update_frequency: int = 0


@dataclass(frozen=True)
class RepliesConfig:
    update: 'Iterable[RepliesUpdateConfig]' = ()
    update_frequency: int = 0


@dataclass(frozen=True)
class RepliesModuleConfig:
    repliesConfig: RepliesConfig = RepliesConfig()
