from typing import Any
from comments.comments_config import CommentsConfig, CommentsModuleConfig, CommentsUpdateConfig, VideoCommentsConfig, VideoCommentsUpdateConfig

CommentsUpdateConfig
CommentsConfig
VideoCommentsUpdateConfig
VideoCommentsConfig
CommentsModuleConfig


def comments_update_config_parser(config: Any):
    return [CommentsUpdateConfig(comment_id, config.get(comment_id, CommentsUpdateConfig.update_frequency)) for comment_id in config]


def comments_config_parser(comments: Any):
    return CommentsConfig(
        update=(update := comments.get("update")) and comments_update_config_parser(
            update) or CommentsConfig.update,
        update_frequency=comments.get(
            "update_frequency", CommentsConfig.update_frequency)
    )


def video_comments_update_config_parser(config: Any):
    return [VideoCommentsUpdateConfig(video_id, config.get(video_id, VideoCommentsUpdateConfig.update_frequency)) for video_id in config]


def video_comments_config_parser(videos_comments: Any):
    return VideoCommentsConfig(
        update=(update := videos_comments.get("update")) and video_comments_update_config_parser(
            update) or VideoCommentsConfig.update,
        update_frequency=videos_comments.get(
            "update_frequency", VideoCommentsConfig.update_frequency)
    )


def comments_module_config_parser(config: Any):
    return CommentsModuleConfig(
        commentsConfig=(channel_videos := config.get(
            "comments")) and comments_config_parser(channel_videos) or CommentsModuleConfig.commentsConfig,
        videoCommentsConfig=(video_comments := config.get("video_comments")) and video_comments_config_parser(
            video_comments) or CommentsModuleConfig.videoCommentsConfig
    )
