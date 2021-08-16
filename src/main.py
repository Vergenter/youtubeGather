import videos.config_parser
import videos.videos
import channels.config_parser
import channels.channels
import comments.config_parser
import comments.comments
import replies.config_parser
import replies.replies
import os
from typing import Callable,  TypeVar
import yaml
from typing import Any, Callable, TypeVar


T = TypeVar("T")
videos


def runer_from_config(config_parser: Callable[[Any], T], main_function: Callable[[T], None]):
    def inner_runer(config: Any):
        main_function(config_parser(config))
    return inner_runer


modules: 'list[Callable[[Any],None]]' = [
    runer_from_config(
        videos.config_parser.videos_module_config_parser, videos.videos.main),
    runer_from_config(
        channels.config_parser.channels_module_config_parser, channels.channels.main),
    runer_from_config(
        comments.config_parser.comments_module_config_parser, comments.comments.main),
    runer_from_config(
        replies.config_parser.replies_module_config_parser, replies.replies.main)
]


def main():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    config_path = 'config//config.yaml'
    file_path = os.path.join(dir_path, config_path)
    with open(file_path, 'r', encoding='utf-8') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        for module in modules:
            module(config)


if __name__ == "__main__":
    main()
