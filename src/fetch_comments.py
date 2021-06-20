from typing import cast
import datetime
import logging
from db.connection import Neo4jConnection
from models.Comment_dt import Video_dc, comment_query, fromJson_dc
from process.comment_process import process_commandThread
from acquire.comment_api import query_by_video_id
logging.basicConfig(filename='logs//comments_gather.log', level=logging.INFO)


def main():
    logging.info("Fetching comments started %s", datetime.datetime.now())
    conn = Neo4jConnection()
    video_ids = ["fj28UtF0-Fs", "k4v6slOxxXs", "sJW4Le1CX-g", "hDkuUZ3F1GU", "-As3w9Hhl88",
                 "cT7wOSOZVoc", "RJ0jdO5ZfU4"]
    logging.info("Fetching comments for %d videos", len(video_ids))
    quota_exceeded = False
    for video_id in video_ids:
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
