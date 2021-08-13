from dataclasses import asdict
import os
from typing import Callable, Type, TypeVar, Union
from config.config import read_config
import yaml
from search import search, search_config
from include import include, include_config
from channels import channels, channels_config
from videos import videos, videos_config
from comments import comments, comments_config
import ray
modules = {
    search_config.search: search.main,
    include_config.include: include.main,
    channels_config.channels: channels.main,
    videos_config.videos: videos.main,
    comments_config.comments: comments.main
}


def main():
    ray.init(address='auto', _redis_password='5241590000000000')
    dir_path = os.path.dirname(os.path.realpath(__file__))
    config_path = 'config//policy.yaml'
    file_path = os.path.join(dir_path, config_path)
    with open(file_path, 'r', encoding='utf-8') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        for model, module in modules.items():
            data = read_config(config, model)
            if data != None:
                module(data)


if __name__ == "__main__":
    main()
