from typing import cast
import datetime
import logging
from db.connection import Neo4jConnection
from models.Comment_dt import Video_dc, comment_query, fromJson_dc
from process.comment_process import process_commandThread
from acquire.comment_api import query_by_video_id

channels_query = '''
match (ch:Channel)--(v:Video) 
where ch.channelId in $channels AND NOT (v)--(:Comment)
return v.videoId
'''
videos_by_id_query = '''
match (v:Video) 
where v.videoId in $videos AND NOT (v)--(:Comment)
return v.videoId
'''
videos_query = '''
match (v:Video) 
where NOT (v)--(:Comment)
return v.videoId
'''


def main(video_ids: 'list[str]' = [], channel_ids: 'list[str]' = [], all: bool = False):
    logging.basicConfig(
        filename='logs//comments_gather.log', level=logging.INFO, force=True)
    logging.info("Fetching comments started %s", datetime.datetime.now())
    conn = Neo4jConnection()

    videos = set()
    if all:
        logging.warning(
            "Fetching comments for all videos isn't secured for videos with no comments, which will led to wasting quota for them")
        videos = set([v[0] for v in conn.query(videos_query)])
    else:
        if len(video_ids) > 0:
            videos_videos_ids = [v[0] for v in conn.query(
                videos_by_id_query, {"videos": video_ids})]
            videos.update(set(videos_videos_ids))
        if len(channel_ids) > 0:
            channel_videos_ids = [v[0] for v in conn.query(
                channels_query, {"channels": channel_ids})]
            videos.update(set(channel_videos_ids))
    # videos = ["fj28UtF0-Fs", "k4v6slOxxXs", "sJW4Le1CX-g", "hDkuUZ3F1GU", "-As3w9Hhl88",
    #              "cT7wOSOZVoc", "RJ0jdO5ZfU4"]
    logging.info("Fetching comments for %d videos", len(videos))
    quota_exceeded = False
    for video_id in videos:
        jsons, quota_exceeded = query_by_video_id(video_id)
        rows = list(map(fromJson_dc, process_commandThread(jsons)))
        conn.bulk_insert_data(
            comment_query, [comment for comment_thread in rows for comment in comment_thread])
        if quota_exceeded:
            break
    conn.close()
    if quota_exceeded:
        logging.info("Fetching comments partialy failled %s",
                     datetime.datetime.now())
    else:
        logging.info("Fetching comments succeeded %s", datetime.datetime.now())


if __name__ == "__main__":
    main()
