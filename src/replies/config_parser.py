from typing import Any

from replies.replies_config import RepliesConfig, RepliesModuleConfig, RepliesUpdateConfig


def replies_update_config_parser(config: Any):
    return [RepliesUpdateConfig(comment_id, config.get(comment_id, RepliesUpdateConfig.update_frequency)) for comment_id in config]


def replies_config_parser(replies: Any):
    return RepliesConfig(
        update=(update := replies.get("update")) and replies_update_config_parser(
            update) or RepliesConfig.update,
        update_frequency=replies.get(
            "update_frequency", RepliesConfig.update_frequency)
    )


def replies_module_config_parser(config: Any):
    return RepliesModuleConfig(
        repliesConfig=(replies := config.get(
            "replies")) and replies_config_parser(replies) or RepliesModuleConfig.repliesConfig
    )
