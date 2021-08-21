from dataclasses import dataclass
from typing import Iterable

from utils.types import CommentId, VideoId


@dataclass(frozen=True)
class CommentsConfig:
    videos_comments_update_frequency: int = 0
    comment_update_frequency: int = 0


def from_yaml(config):
    return CommentsConfig(
        videos_comments_update_frequency=config.get(
            "videos_comments_update_frequency", CommentsConfig.videos_comments_update_frequency),
        comment_update_frequency=config.get(
            "comment_update_frequency", CommentsConfig.comment_update_frequency)
    )
