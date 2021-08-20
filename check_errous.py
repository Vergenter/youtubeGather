import json
from videos.video_model import from_json


def main():
    with open('errous.json', 'r') as f:
        errous = json.load(f)
        for x in errous:
            from_json(x)


if __name__ == "__main__":
    main()
