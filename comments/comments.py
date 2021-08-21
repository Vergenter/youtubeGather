from comments_config import from_yaml, CommentsConfig
import yaml


def main(data: CommentsConfig):
    print(data)


if __name__ == "__main__":
    with open("config.yaml", 'r', encoding='utf-8') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        main(from_yaml(config))
