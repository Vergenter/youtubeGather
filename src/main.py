import yaml
from fetch_comments import main as fetch_comments
from fetch_replies import main as fetch_replies
from fetch_videos import main as fetch_videos
from label_sentiment import main as label_sentiment
from search_videos import main as search_videos


def isPolicyActive(file, policy: str):
    return file.get(policy, {"state": False}).get("state", False)


def getInheritableData(comments_policy):
    return (
        comments_policy.get("video-ids", []),
        comments_policy.get("channel-ids", []),
        comments_policy.get("all", False)
    )


def tryInherit(policy, inherit_policy):
    video_ids = []
    channel_ids = []
    all = False
    if policy.get("inherit", False) and inherit_policy != None:
        video_ids, channel_ids, all = getInheritableData(
            inherit_policy)
    else:
        video_ids = policy.get("video-ids", [])
        channel_ids = policy.get("channel-ids", [])
        all = policy.get("all", False)
    return (video_ids, channel_ids, all)


def main():
    with open('config//policy.yaml', 'r', encoding='utf-8') as f:
        policy = yaml.load(f, Loader=yaml.FullLoader)

        if isPolicyActive(policy, "search"):
            search_policy = policy["search"]
            default_limit = search_policy.get("default-limit", 0)
            default_languages = search_policy.get("default-limit", ["en"])
            keywords = [(key["keyword"], key["games"], key.get("languages", default_languages), key.get(
                "limit", default_limit)) for key in search_policy.get("keywords", [])]
            search_videos(keywords)

        if isPolicyActive(policy, "videos"):
            channel_videos_policy = policy["videos"]
            video_ids = channel_videos_policy.get("video-ids", [])
            languages = channel_videos_policy.get("languages", [])
            games = channel_videos_policy.get("games", [])
            fetch_videos(video_ids, games, languages)

        if isPolicyActive(policy, "comments"):
            comments_policy = policy["comments"]
            video_ids, channel_ids, all = getInheritableData(comments_policy)
            fetch_comments(video_ids, channel_ids, all)

        if isPolicyActive(policy, "replies"):
            replies_policy = policy["replies"]
            video_ids, channel_ids, all = tryInherit(
                replies_policy, policy.get("comments"))
            min_replies = replies_policy.get("min-replies", 50)
            fetch_replies(video_ids, channel_ids, all, min_replies)

        if isPolicyActive(policy, "sentiment"):
            sentimemt_policy = policy["sentiment"]
            video_ids, channel_ids, all = tryInherit(
                sentimemt_policy, policy.get("comments"))
            label_sentiment(video_ids, channel_ids, all)

        if isPolicyActive(policy, "channels"):
            raise NotImplementedError
            channels_policy = policy["channels"]


if __name__ == "__main__":
    main()
