from dataclasses import dataclass
from typing import Iterable

from utils.types import CommentId, VideoId


@dataclass(frozen=True)
class CommentsUpdateConfig:
    comment_id: CommentId
    update_frequency: int = 0


@dataclass(frozen=True)
class CommentsConfig:
    update: 'Iterable[CommentsUpdateConfig]' = ()
    update_frequency: int = 0


@dataclass(frozen=True)
class VideoCommentsUpdateConfig:
    video_id: VideoId
    update_frequency: int = 0


@dataclass(frozen=True)
class VideoCommentsConfig:
    update: 'Iterable[VideoCommentsUpdateConfig]' = ()
    update_frequency: int = 0


@dataclass(frozen=True)
class CommentsModuleConfig:
    commentsConfig: CommentsConfig = CommentsConfig()
    videoCommentsConfig: VideoCommentsConfig = VideoCommentsConfig()
